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
package org.apache.beam.sdk.extensions.timeseries.fs.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.timeseries.fs.MutableState;

/**
 * Naive implementation of an order book object used for testing purposes.
 *
 * <p>Actions are add, cancel and modify, with a simple TreeMap used to store values based on the
 * {@link Order} value as the Tree index.
 *
 * <p>TODO remove java serializable as coder
 */
public class NaiveOrderBook extends OrderBook<Tick> implements Serializable {
  // TODO figure out how to avoid NullChecker errors for Order.id AutoValue Not Nullable property
  public TreeMap<Double, Map<String, Order>> bids = new TreeMap<>(Comparator.reverseOrder());
  public TreeMap<Double, Map<String, Order>> asks = new TreeMap<>();

  @Override
  public NaiveOrderBook add(Order order) {
    String id = order.getId();
    if (id != null) {
      getTree(order.isBid()).computeIfAbsent(order.getValue(), s -> new HashMap<>()).put(id, order);
    }
    return this;
  }

  @Override
  public NaiveOrderBook cancel(Order order) {
    String id = order.getId();
    if (id != null) {
      Optional.ofNullable(getTree(order.isBid()).get(order.getValue()))
          .ifPresent(a -> a.remove(id));
    }
    return this;
  }

  @Override
  public NaiveOrderBook modify(Order originalOrder, Order modifiedOrder) {

    String id = modifiedOrder.getId();
    if (id != null) {
      Optional.ofNullable(getTree(originalOrder.isBid()).get(originalOrder.getValue()))
          .ifPresent(a -> a.remove(id));
      this.add(modifiedOrder);
    }
    return this;
  }

  @Override
  public List<Order> getAsks() {
    List<Order> asksList = new ArrayList<>();
    for (Map.Entry<Double, Map<String, Order>> o : asks.entrySet()) {
      asksList.addAll(o.getValue().values());
    }
    return asksList;
  }

  @Override
  public MutableState<Tick> append(Tick tick) {
    return this;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NaiveOrderBook)) {
      return false;
    }
    NaiveOrderBook that = (NaiveOrderBook) o;
    //    LoggerFactory.getLogger(NaiveOrderBook.class).info("Compare! {} {}  ", asks, that.asks);
    return Objects.equals(bids, that.bids) && Objects.equals(asks, that.asks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bids, asks);
  }

  private TreeMap<Double, Map<String, Order>> getTree(Boolean isBid) {
    return isBid ? bids : asks;
  }

  @Override
  public void mutate(Tick mutation) {
    Optional.ofNullable(mutation.getOrder()).ifPresent(this::add);
  }
}
