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

// TODO use different Coder
public class Tick implements Serializable {
  @Nullable Long globalSequence;
  @Nullable String id;
  @Nullable Order order;

  public Tick(Long globalSequence, String id, Order order) {
    this.globalSequence = globalSequence;
    this.id = id;
    this.order = order;
  }

  public Tick() {}

  public Tick setGlobalSequence(Long value) {
    globalSequence = value;
    return this;
  };

  public Tick setId(String value) {
    id = value;
    return this;
  }

  public Tick setOrder(Order value) {
    order = value;
    return this;
  }

  public @Nullable Long getGlobalSequence() {
    return this.globalSequence;
  };

  @Nullable
  public String getId() {
    return this.id;
  }

  @Nullable
  public Order getOrder() {
    return this.order;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Tick)) {
      return false;
    }
    Tick tick = (Tick) o;
    return Objects.equals(globalSequence, tick.globalSequence)
        && Objects.equals(id, tick.id)
        && Objects.equals(order, tick.order);
  }

  @Override
  public int hashCode() {
    return Objects.hash(globalSequence, id, order);
  }
}
