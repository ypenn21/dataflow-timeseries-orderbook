/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.timeseries.fs;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base transform that will accept raw input of data in the general format of (sequence, data) where
 * sequence is a monotonically increasing value. Data will include an id that identifies an
 * instrument.
 *
 * <p>Multiplex mode is where symbols come with a GlobalSequenceId and can have many symbols
 *
 * <p>[(0, G), (1,A), (2,G), (3,G), (4,A)]
 *
 * <p>Simplex is where each symbol comes with its own Sequence ID. This mode is NOT IMPLEMENTED [(0,
 * G), (0,A), (1,G), (2,G), (1,A)]
 *
 * <p>Multiplex Mode:
 *
 * <p>This impl makes use of event time timers to sync the release of values per symbol based on the
 * data seen from the GlobalSequence. Sequence is expected to be monotonically increasing by a known
 * value at each step.
 *
 * <OL>
 *   TODO : Currently we assume SeqNow-SeqPrev = 1, add lambda
 *   <p>TODO : Add start seqnum logic, right now min(first window) becomes start seqnum
 *   <p>TODO : Objects for the G * TODO : Detect size of OrderBook object, the maximum size of this
 *   object in state is runner dependent
 * </OL>
 *
 * Note this impl assumes the GlobalSeq will not be larger than {Long.Max}
 *
 * <p>An internal Batch length in seconds dictates the maximum frequency by which the Sync call is
 * pushed. The push is in the format of a SideInput which is sent to all symbol-state machines. Each
 * symbol-state will hold values in an {OrderedListState} until the release signal.
 *
 * <p>Note the system will halt processing if a global sequence gap is detected.
 *
 * <p>Note currently the windowing strategy of the PCollection into the transform is not preserved.
 *
 * <p>The input is expected to be in the format : KV<Long, KV<String, T>> The first Key is the
 * GlobalSeqValue and the next key is the symbol.
 */
@AutoValue
@SuppressWarnings({"nullness", "TypeNameShadowing"})
public abstract class TickerStream<T, K extends MutableState<T>>
    extends PTransform<PCollection<KV<Long, KV<String, T>>>, PCollection<K>> {

  public abstract Mode getMode();

  public abstract Class<K> getMutableStateClazz();

  public abstract Coder<T> getMutationCoder();

  @Nullable
  public abstract Long getEstimateFirstSeqNumSize();

  public static <T, V extends MutableState<T>> TickerStream<T, V> create(
      Mode mode, Class<V> clazz, Coder<T> coder) {

    checkArgumentNotNull(mode, "Mode must be set.");
    checkArgumentNotNull(clazz, "Class of MutableSet must be set. Generics eh...");
    checkArgumentNotNull(coder, "Coder of Mutation must be set. Generics eh...");

    assert !mode.equals(Mode.SIMPLEX_STREAM);

    return new AutoValue_TickerStream.Builder<T, V>()
        .setMode(mode)
        .setMutableStateClazz(clazz)
        .setMutationCoder(coder)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder<T, K extends MutableState<T>> {
    public abstract Builder<T, K> setMode(Mode value);

    public abstract Builder<T, K> setMutableStateClazz(Class<K> value);

    public abstract Builder<T, K> setMutationCoder(Coder<T> value);

    public abstract Builder<T, K> setEstimateFirstSeqNumSize(Long value);

    public abstract TickerStream<T, K> build();
  }

  // This value dictates how often the Sync event is broadcast
  static final Duration BATCH_DURATION = Duration.standardSeconds(1);

  // Stable name for the SIDE INPUT which is also used directly in Unit tests
  static final String SIDE_INPUT_NAME = "GlobalSeqWM";

  /** Storing window strategy of incoming stream, to allow reapplication post transform. */
  @Nullable private WindowingStrategy<?, ?> incomingWindowStrategy = null;

  public @Nullable WindowingStrategy<?, ?> getIncomingWindowStrategy() {
    return this.incomingWindowStrategy;
  }

  public void setIncomingWindowStrategy(WindowingStrategy<?, ?> value) {
    this.incomingWindowStrategy = value;
  }

  @Override
  public PCollection<K> expand(PCollection<KV<Long, KV<String, T>>> input) {

    // TODO reset window strategy
    setIncomingWindowStrategy(input.getWindowingStrategy());

    // TODO Create Validation Transform, check seq id is set etc..
    // TODO Reduce byte[] to the single thread
    // This step will push all values to a single thread for processing of the GlobalTicks
    // TODO Behaviour of an Iterable is that it will grow forever, this will eventually fail / OOM
    // Need to switch to Singleton

    PCollectionView<Iterable<KV<Instant, Long>>> i =
        input
            .apply(
                "ApplyGlobalWindow",
                Window.<KV<Long, KV<String, T>>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes())
            .apply(
                "MapToKV<1,Tick>",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.longs()))
                    .via(x -> KV.of(1, x.getKey())))
            .apply("TrackGlobalSequence", ParDo.of(new GlobalSeqWM(TickerStream.BATCH_DURATION)))
            .apply(View.asIterable());

    Coder<K> orderBookCoder = null;

    // This will fail at Pipeline creation time.
    try {
      orderBookCoder = input.getPipeline().getCoderRegistry().getCoder(getMutableStateClazz());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }

    // This is the main path, with Ticks waiting in state until cleared.

    return input
        .apply(
            "MapToKV<String,Mutation>",
            MapElements.<KV<String, KV<Long, T>>>into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(),
                        TypeDescriptors.kvs(
                            TypeDescriptors.longs(),
                            getMutationCoder().getEncodedTypeDescriptor())))
                .via(x -> KV.of(x.getValue().getKey(), KV.of(x.getKey(), x.getValue().getValue()))))
        .apply("ApplyGlobalWindowMain", Window.into(new GlobalWindows()))
        .apply(
            ParDo.of(new SymbolState<T, K>(this, orderBookCoder))
                .withSideInput(TickerStream.SIDE_INPUT_NAME, i))
        .setCoder(orderBookCoder);
  }

  /**
   * Per Symbol State, holding value until a SideInput signal to release up to a GlobalSequenceNum.
   *
   * <p>Input signal is {KV<Instant,Long>} which is a range of [0, LongValue)
   */
  public static class SymbolState<T, K extends MutableState<T>>
      extends DoFn<KV<String, KV<Long, T>>, K> {

    // Config
    private TickerStream<T, K> tickerStream;

    private static final Logger LOG = LoggerFactory.getLogger(SymbolState.class);

    public SymbolState(TickerStream<T, K> tickerStream, Coder<K> stateStateSpec) {
      checkArgumentNotNull(tickerStream);
      this.tickerStream = tickerStream;
      orderBook = StateSpecs.value(stateStateSpec);
      buffer = StateSpecs.orderedList(tickerStream.getMutationCoder());
    }

    // Buffers Ticks until we get signal to release
    @DoFn.StateId("buffer")
    @SuppressWarnings("unused")
    private final StateSpec<OrderedListState<T>> buffer;

    // SideInput used to store the release signals
    @DoFn.StateId("releaseSignals")
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Map<Instant, Long>>> seqWMs =
        StateSpecs.value(MapCoder.of(InstantCoder.of(), VarLongCoder.of()));

    // Order book object stored as Singleton
    @SuppressWarnings("unused")
    @DoFn.StateId("orderBook")
    private final StateSpec<ValueState<K>> orderBook;

    @TimerFamily("release")
    @SuppressWarnings("unused")
    private final TimerSpec expirySpec = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    // Buffers Ticks until we get signal to release
    @DoFn.StateId("currSeq")
    @SuppressWarnings("unused")
    private final StateSpec<CombiningState<Long, long[], Long>> currSeq =
        StateSpecs.combining(Max.ofLongs());

    @ProcessElement
    public void process(
        @SideInput(TickerStream.SIDE_INPUT_NAME) Iterable<KV<Instant, Long>> sideInputSignals,
        @StateId("releaseSignals") ValueState<Map<Instant, Long>> releaseSignals,
        @StateId("buffer") OrderedListState<T> buffer,
        @StateId("currSeq") CombiningState<Long, long[], Long> currSeq,
        @TimerFamily("release") TimerMap expiryTimers,
        @Element KV<String, KV<Long, T>> tick) {

      // TODO Validation of Ticks in expand will prevent NPE error here
      long sequence = tick.getValue().getKey();

      // Add elements to OrderedList, we do not do any processing @Process only in the EventTimer
      buffer.add(TimestampedValue.of(tick.getValue().getValue(), Instant.ofEpochMilli(sequence)));

      Map<Instant, Long> signals = new HashMap<>();

      boolean newTriggers = false;
      for (KV<Instant, Long> k : sideInputSignals) {
        if (k.getValue() > currSeq.read()) {
          newTriggers = true;
          signals.put(k.getKey(), k.getValue());
          // Set timer for Release time
          expiryTimers.set(String.valueOf(k.getKey().getMillis()), k.getKey());
          currSeq.add(k.getValue());
          releaseSignals.write(signals);
        }
      }
      if (newTriggers) {
        signals.putAll(releaseSignals.read());
        releaseSignals.write(signals);
      }
    }

    @OnTimerFamily("release")
    public void onRelease(
        OutputReceiver<K> context,
        OnTimerContext timerContext,
        @Key String key,
        @StateId("releaseSignals") ValueState<Map<Instant, Long>> releaseSignals,
        @StateId("orderBook") ValueState<K> orderBookState,
        @StateId("buffer") OrderedListState<T> buffer) {

      // This should never be null, we can only be here if a value was set
      Map<Instant, Long> signals = releaseSignals.read();

      LOG.info(
          "Lets get all values - key {} - {} - {} - {} ",
          key,
          signals.isEmpty(),
          timerContext.timestamp(),
          signals.get(timerContext.timestamp()));

      if (Boolean.FALSE.equals(buffer.isEmpty().read())) {
        Long releaseSignal = signals.get(timerContext.timestamp());

        Iterable<TimestampedValue<T>> batch =
            buffer.readRange(Instant.EPOCH, Instant.ofEpochMilli(releaseSignal));

        K orderBook;

        try {
          orderBook =
              Optional.ofNullable(orderBookState.read())
                  .orElse(
                      tickerStream.getMutableStateClazz().getDeclaredConstructor().newInstance());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        int count = 0;
        for (TimestampedValue<T> tickTimestampedValue : batch) {
          T tick = tickTimestampedValue.getValue();
          orderBook.mutate(tick);
          count++;
        }

        if (count > 0) {
          context.outputWithTimestamp(orderBook, timerContext.timestamp());
          orderBookState.write(orderBook);
        }

        // Note Range is [0, releaseSignal)
        buffer.clearRange(Instant.EPOCH, Instant.ofEpochMilli(releaseSignal));
        // Clear TimerMapValue
        signals.remove(timerContext.timestamp());
        releaseSignals.write(signals);
      }
    }
  }

  public static class GlobalSeqWM extends DoFn<KV<Integer, Long>, KV<Instant, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalSeqWM.class);

    private Duration batchSize;

    public Duration getBatchSize() {
      return batchSize;
    }

    public <T, K extends MutableState<T>> GlobalSeqWM(Duration batchSize) {
      this.batchSize = batchSize;
    }

    @DoFn.StateId("buffer")
    @SuppressWarnings("unused")
    private final StateSpec<OrderedListState<TimestampedValue<Long>>> buffer =
        StateSpecs.orderedList(TimestampedValue.TimestampedValueCoder.of(VarLongCoder.of()));

    @DoFn.StateId("timerSet")
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Boolean>> timerState = StateSpecs.value(BooleanCoder.of());

    @StateId("seqWaterMark")
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Long>> seqWaterMark = StateSpecs.value(VarLongCoder.of());

    @StateId("maxSequence")
    @SuppressWarnings("unused")
    private final StateSpec<CombiningState<Long, long[], Long>> maxSequence =
        StateSpecs.combining(Max.ofLongs());

    @StateId("minSequence")
    @SuppressWarnings("unused")
    private final StateSpec<CombiningState<Long, long[], Long>> minSequence =
        StateSpecs.combining(Min.ofLongs());

    @TimerId("onTimer")
    @SuppressWarnings("unused")
    private final TimerSpec onTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @StateId("buffer") OrderedListState<TimestampedValue<Long>> buffer,
        @StateId("timerSet") ValueState<Boolean> isTimerSet,
        @StateId("maxSequence") CombiningState<Long, long[], Long> maxSequence,
        @StateId("minSequence") CombiningState<Long, long[], Long> minSequence,
        @TimerId("onTimer") Timer expiryTimer,
        @Timestamp Instant eventTime,
        @Element KV<Integer, Long> globalSeq) {

      long sequence = globalSeq.getValue();
      maxSequence.add(sequence);
      minSequence.add(sequence);

      // Add elements to OrderedList, we do not do any processing @Process only in the EventTimer
      buffer.add(
          TimestampedValue.of(
              TimestampedValue.of(globalSeq.getValue(), eventTime),
              Instant.ofEpochMilli(sequence)));

      if (!Optional.ofNullable(isTimerSet.read()).orElse(false)) {
        expiryTimer
            .withOutputTimestamp(eventTime.plus(getBatchSize()))
            .set(eventTime.plus(getBatchSize()));
        isTimerSet.write(true);
        LOG.trace("Setting Timers to {}", eventTime.plus(getBatchSize()));
      }
    }

    @OnTimer("onTimer")
    public void onTimer(
        OutputReceiver<KV<Instant, Long>> context,
        OnTimerContext timerContext,
        @StateId("seqWaterMark") ValueState<Long> seqWaterMarkState,
        @StateId("buffer") OrderedListState<TimestampedValue<Long>> buffer,
        @StateId("maxSequence") CombiningState<Long, long[], Long> maxSequence,
        @StateId("minSequence") CombiningState<Long, long[], Long> minSeq,
        @StateId("timerSet") ValueState<Boolean> isTimerSet,
        @TimerId("onTimer") Timer timer) {

      // TODO making assumption that the sequence starts from a positive number that is increasing
      // -1 is magic number which indicates this is the first ever value
      long seqWM = Optional.ofNullable(seqWaterMarkState.read()).orElse(-1L);
      Instant endRead = Instant.ofEpochMilli(seqWM + getBatchSize().getMillis());
      // If internalWaterMark is -1 then we are in startup mode
      if (seqWM == -1) {
        // We have to page through the list buffer, as we are mapping global seq onto timestamp,
        // find
        // min value
        Instant readSize = Instant.ofEpochMilli(minSeq.read() + getBatchSize().getMillis());
        endRead = readSize.isAfter(endRead) ? readSize : endRead;
      }
      minSeq.clear();

      if (Boolean.FALSE.equals(buffer.isEmpty().read())) {

        Iterable<TimestampedValue<TimestampedValue<Long>>> batch =
            buffer.readRange(Instant.ofEpochMilli(seqWM == -1 ? 0 : seqWM), endRead);

        Iterator<TimestampedValue<TimestampedValue<Long>>> batchItr = batch.iterator();

        boolean noGap = true;
        long lastSeq = seqWM;

        while (noGap && batchItr.hasNext()) {

          // Recall timestamp here is just a proxy for sequence number
          TimestampedValue<TimestampedValue<Long>> current = batchItr.next();
          long nextSeq = current.getTimestamp().getMillis();

          if ((current.getValue().getTimestamp().isBefore(timerContext.timestamp()))
              && (lastSeq == -1 || (nextSeq - lastSeq) == 1)) {
            lastSeq = nextSeq;

          } else {
            noGap = false;
          }
        }

        if (lastSeq != seqWM) {
          context.output(KV.of(timerContext.timestamp(), lastSeq + 1));
          LOG.info("Releasing global watermark based on contiguous output to {}", lastSeq);
        }

        buffer.clearRange(
            Instant.ofEpochMilli(seqWM == -1 ? 0 : seqWM), Instant.ofEpochMilli(lastSeq + 1));

        // Update our internal watermark to be the last good sequence
        seqWaterMarkState.write(lastSeq);

        // Are we at the end of our BagState

        if (Boolean.TRUE.equals(buffer.isEmpty().read()) && (lastSeq == maxSequence.read())) {
          // Then we unset the timerIsSet Value
          isTimerSet.write(false);
        } else {
          // We need to set a timer for n sec from now
          timer
              .withOutputTimestamp(timerContext.timestamp().plus(getBatchSize()))
              .set(timerContext.timestamp().plus(getBatchSize()));
          LOG.info(
              "Setting a new Timer from existing timer which fired at {}",
              timerContext.timestamp());
        }
      }
    }
  }

  public enum Mode {
    SIMPLEX_STREAM,
    MULTIPLEX_STREAM
  }
}
