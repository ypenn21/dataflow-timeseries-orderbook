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
import java.util.Objects;
import javax.annotation.Nullable;

public class Order implements Serializable {

  private String id;

  private Double value;

  boolean bid;

  private TYPE type;

  public Order(String id, Double value, boolean bid, TYPE type) {
    this.id = id;
    this.value = value;
    this.bid = bid;
    this.type = type;
  }

  public String getId() {
    return id;
  }

  public Order setId(String id) {
    this.id = id;
    return this;
  }

  public Double getValue() {
    return value;
  }

  public Order setValue(Double value) {
    this.value = value;
    return this;
  }

  public boolean isBid() {
    return bid;
  }

  public Order setBid(boolean bid) {
    this.bid = bid;
    return this;
  }

  public @Nullable TYPE getType() {
    return type;
  }

  public Order setType(TYPE type) {
    this.type = type;
    return this;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Order)) {
      return false;
    }
    Order order = (Order) o;
    return bid == order.bid
        && Objects.equals(id, order.id)
        && Objects.equals(value, order.value)
        && type == order.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, value, bid, type);
  }

  @Override
  public String toString() {
    return "Order{"
        + "id='"
        + id
        + '\''
        + ", value="
        + value
        + ", bid="
        + bid
        + ", type="
        + type
        + '}';
  }

  public enum TYPE {
    ADD,
    CANCEL,
    UPDATE
  }
}
