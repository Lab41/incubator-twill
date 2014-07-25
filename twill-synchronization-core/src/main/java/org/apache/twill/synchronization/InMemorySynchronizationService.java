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
package org.apache.twill.synchronization;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple in memory implementation of {@link SynchronizationService}.
 */
public class InMemorySynchronizationService implements SynchronizationService {

  private final Map<String, DoubleBarrierWrapper> barriers = Maps.newHashMap();
  private final Lock lock = new ReentrantLock();

  @Override
  public DoubleBarrier getDoubleBarrier(String name, int parties) {
    lock.lock();
    try {
      DoubleBarrierWrapper barrier = barriers.get(name);
      if (barrier == null) {
        barrier = new DoubleBarrierWrapper(parties);
        barriers.put(name, barrier);
      }
      return barrier;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Inner class that provides a barrier interface.
   */
  private class DoubleBarrierWrapper implements DoubleBarrier {

    CyclicBarrier enterBarrier;
    CyclicBarrier leaveBarrier;

    public DoubleBarrierWrapper(int parties) {
      this.enterBarrier = new CyclicBarrier(parties);
      this.leaveBarrier = new CyclicBarrier(parties);
    }

    @Override
    public int getParties() {
      return enterBarrier.getParties();
    }

    @Override
    public void enter() throws Exception {
      enterBarrier.await();
    }

    @Override
    public void enter(long maxWait, TimeUnit unit) throws Exception {
      enterBarrier.await(maxWait, unit);
    }

    @Override
    public void leave() throws Exception {
      leaveBarrier.await();
    }

    @Override
    public void leave(long maxWait, TimeUnit unit) throws Exception {
      leaveBarrier.await(maxWait, unit);
    }
  }
}
