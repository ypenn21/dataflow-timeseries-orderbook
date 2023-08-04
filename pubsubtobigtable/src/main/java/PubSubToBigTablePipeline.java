/*
 Copyright 2023 Google LLC
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
      https://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

// Imports 
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

public class PubSubToBigTablePipeline {
    public static final Logger log = LoggerFactory.getLogger(PubSubToBigTablePipeline.class);

    public interface BigTableOptions extends GcpOptions {
        @Description("PubSub Topic to read from")
        String getTopic();

        void setTopic(String value);

        // For simplicity of testing, we are going to keep the project consistent for
        // all resources
        // If we wanted to add more flexibility, we could enable project options here
        // @Description("The Bigtable project ID, this can be different than your
        // Dataflow project")
        // @Default.String("ibnd-argls-cstmr-demos")
        // String getBigtableProjectId();
        // void setBigtableProjectId(String bigtableProjectId);

        @Description("The Bigtable instance ID")
        String getBigtableInstanceId();

        void setBigtableInstanceId(String bigtableInstanceId);

        @Description("The Bigtable table ID in the instance.")
        String getBigtableTableId();

        void setBigtableTableId(String bigtableTableId);

    }

    public static PipelineResult execute(BigTableOptions options,
            CloudBigtableTableConfiguration bigtableTableConfig) {
        Pipeline pipeline = Pipeline.create(options);
        Counter counter = Metrics.counter("namespace", "trow-counter");

        PCollection<PubsubMessage> messages = pipeline.apply("Read Messages",
                PubsubIO.readMessagesWithAttributes()
                        .fromSubscription(String.format("projects/%s/subscriptions/%s", options.getProject(),
                                String.format("%s-%s", options.getTopic(), "sub"))));

        PCollection<String> jsonMessages = messages.apply("Convert PubSub to String JSON",
                ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx) {
                        String jsonMessage = new String(ctx.element().getPayload());
                        ctx.output(jsonMessage);
                    }
                }));

        PCollection<KV<ByteString, Iterable<Mutation>>> rows = jsonMessages.apply("Convert to BT row",
                ParDo.of(new DoFn<String, KV<ByteString, Iterable<Mutation>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx) throws JsonProcessingException {
                        counter.inc();

                        Map m = new ObjectMapper().readValue(ctx.element(), Map.class);
                        /*
                         * TODO
                         * Determine correct rowkey
                         */
                        String rowkey = (String) m.get("inst_exch_mrkt_id") + "#" + m.get("txn_tmsp");

                        ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();

                        for (Object key : m.keySet()) {
                            // Create a mutation for every column in a row
                            /*
                             * TODO
                             * Set proper data types
                             */
                            mutations.add(Mutation.newBuilder()
                                    .setSetCell(SetCell.newBuilder()
                                            .setFamilyName("cf1")
                                            .setColumnQualifier(ByteString.copyFromUtf8((String) key))
                                            // .setTimestampMicros(System.currentTimeMillis().getTimestampMicros())
                                            // get last version of cell
                                            .setValue(ByteString.copyFromUtf8(m.get(key).toString())))
                                    .build());
                        }

                        ctx.output(KV.of(ByteString.copyFromUtf8(rowkey), mutations.build()));
                    }
                }));

        rows.apply("Write to BT", BigtableIO.write().withProjectId(options.getProject())
                .withInstanceId(options.getBigtableInstanceId()).withTableId(options.getBigtableTableId()));

        return pipeline.run();
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(BigTableOptions.class);
        BigTableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(BigTableOptions.class);

        options.setWorkerRegion("us-central1");
        CloudBigtableTableConfiguration bigtableTableConfig = new CloudBigtableTableConfiguration.Builder()
                .withProjectId(options.getProject())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(options.getBigtableTableId())
                .build();
        execute(options, bigtableTableConfig);
    }
}