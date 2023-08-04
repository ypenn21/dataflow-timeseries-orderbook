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

import static org.junit.Assert.assertEquals;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.beam.sdk.extensions.timeseries.fs.example.NaiveOrderBook;
import org.apache.beam.sdk.extensions.timeseries.fs.example.Order;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NaiveOrderBookTest {

  static final Order ASK_A = new Order("ask_a", 1D, false, Order.TYPE.ADD);

  static final Order ASK_B = new Order("ask_b", 1D, false, Order.TYPE.ADD);
  static final Order ASK_C = new Order("ask_c", 2D, false, Order.TYPE.ADD);
  static final Order BID_A = new Order("bid_a", 1D, true, Order.TYPE.ADD);
  static final Order BID_B = new Order("bid_b", 1D, true, Order.TYPE.ADD);
  static final Order BID_C = new Order("bid_c", 2D, true, Order.TYPE.ADD);

  @Test
  public void naiveOrderBookAddBid() {
    NaiveOrderBook book = new NaiveOrderBook();

    book.add(BID_A);
    book.add(BID_B);
    book.add(BID_C);

    TreeMap<Double, Map<String, Order>> expected = new TreeMap<>(Comparator.reverseOrder());

    final HashMap<String, Order> bid1 = new HashMap<>();
    final HashMap<String, Order> bid2 = new HashMap<>();

    bid1.put(BID_A.getId(), BID_A);
    bid1.put(BID_B.getId(), BID_B);
    bid2.put(BID_C.getId(), BID_C);

    expected.put(1D, bid1);
    expected.put(2D, bid2);

    assertEquals(expected, book.bids);
    assertEquals(Optional.of(2.0), Optional.ofNullable(book.bids.firstKey()));
  }

  @Test
  public void naiveOrderBookAddAsk() {
    NaiveOrderBook book = new NaiveOrderBook();

    book.add(ASK_A);
    book.add(ASK_B);
    book.add(ASK_C);

    TreeMap<Double, Map<String, Order>> expected = new TreeMap<>();
    final HashMap<String, Order> ask1 = new HashMap<>();
    final HashMap<String, Order> ask2 = new HashMap<>();

    ask1.put(ASK_A.getId(), ASK_A);
    ask1.put(ASK_B.getId(), ASK_B);
    ask2.put(ASK_C.getId(), ASK_C);

    expected.put(1.0, ask1);
    expected.put(2.0, ask2);

    assertEquals(expected, book.asks);
    assertEquals(Optional.of(1.0), Optional.ofNullable(book.asks.firstKey()));
  }

  @Test
  public void naiveOrderBookCancelBid() {

    NaiveOrderBook book = new NaiveOrderBook();

    book.add(BID_A);
    book.add(BID_B);
    book.add(BID_C);

    book.cancel(BID_B);

    TreeMap<Double, Map<String, Order>> expected = new TreeMap<>(Comparator.reverseOrder());

    final HashMap<String, Order> bid1 = new HashMap<>();
    final HashMap<String, Order> bid2 = new HashMap<>();

    bid1.put(BID_A.getId(), BID_A);
    bid2.put(BID_C.getId(), BID_C);

    expected.put(1.0, bid1);
    expected.put(2.0, bid2);

    assertEquals(expected, book.bids);
    assertEquals(Optional.of(2.0), Optional.ofNullable(book.bids.firstKey()));
  }

  @Test
  public void naiveOrderBookUpdateAsk() {

    NaiveOrderBook book = new NaiveOrderBook();

    book.add(ASK_A);
    book.add(ASK_B);
    book.add(ASK_C);

    Order mod = new Order(ASK_A.getId(), 2D, ASK_A.isBid(), ASK_A.getType());
    book.modify(ASK_A, mod);

    TreeMap<Double, Map<String, Order>> expected = new TreeMap<>();
    final HashMap<String, Order> ask1 = new HashMap<>();
    final HashMap<String, Order> ask2 = new HashMap<>();

    ask1.put(ASK_B.getId(), ASK_B);
    ask2.put(ASK_C.getId(), ASK_C);
    ask2.put(mod.getId(), mod);

    expected.put(1.0, ask1);
    expected.put(2.0, ask2);

    assertEquals(expected, book.asks);
    assertEquals(Optional.of(1.0), Optional.ofNullable(book.asks.firstKey()));
  }
}
