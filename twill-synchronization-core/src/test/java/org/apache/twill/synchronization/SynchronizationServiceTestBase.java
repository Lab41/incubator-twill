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

import com.google.common.collect.Lists;
import org.apache.twill.common.Cancellable;
import org.apache.twill.zookeeper.ZKOperations;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base class for testing different synchronization service implementations.
 */
public abstract class SynchronizationServiceTestBase {

  private static String BARRIER_NAME = "barrier";
  private static int PARTIES = 3;

  protected abstract SynchronizationService create();

  @Test
  public void testMultiplePartiesEnterAndLeave() throws Exception {
    final SynchronizationService synchronizationService = create();

    ExecutorService executor = Executors.newFixedThreadPool(PARTIES);
    List<Future<Void>> futures = Lists.newArrayList();

    final CountDownLatch postEnterLatch = new CountDownLatch(PARTIES);
    final CountDownLatch postLeaveLatch = new CountDownLatch(PARTIES);

    for (int party = 0; party < PARTIES; ++party) {
      Future<Void> future = executor.submit(
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // Get the barrier.
            DoubleBarrier barrier = synchronizationService.getDoubleBarrier(BARRIER_NAME, PARTIES);
            Assert.assertNotNull(barrier);

            // Propagate up the timeout exceptions.
            barrier.enter(10, TimeUnit.SECONDS);
            postEnterLatch.countDown();
            Assert.assertTrue(postEnterLatch.await(1, TimeUnit.SECONDS));

            // Propagate up the timeout exceptions.
            barrier.leave(10, TimeUnit.SECONDS);
            postLeaveLatch.countDown();
            Assert.assertTrue(postLeaveLatch.await(1, TimeUnit.SECONDS));

            return null;
          }
        }
      );
      futures.add(future);
    }

    for (Future<Void> future: futures) {
      future.get(60, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testFailingToEnterBarrier() throws Exception {
    final SynchronizationService synchronizationService = create();

    ExecutorService executor = Executors.newFixedThreadPool(PARTIES);
    List<Future<Void>> futures = Lists.newArrayList();

    for (int party = 0; party < PARTIES - 1; ++party) {
      Future<Void> future = executor.submit(
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // Get the barrier.
            DoubleBarrier barrier = synchronizationService.getDoubleBarrier(BARRIER_NAME, PARTIES);
            Assert.assertNotNull(barrier);

            // Propagate up the timeout exceptions.

            try {
              barrier.enter(10, TimeUnit.SECONDS);
              Assert.fail();
            } catch (TimeoutException ignored) {
            } catch (BrokenBarrierException ignored) {
            }

            return null;
          }
        }
      );

      futures.add(future);
    }

    for (Future<Void> future: futures) {
      future.get(60, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testFailingToLeaveBarrier() throws Exception {
    final SynchronizationService synchronizationService = create();

    ExecutorService executor = Executors.newFixedThreadPool(PARTIES);
    List<Future<Void>> futures = Lists.newArrayList();

    final CountDownLatch postEnterLatch = new CountDownLatch(PARTIES);

    for (int party = 0; party < PARTIES; ++party) {
      final int p = party;

      Future<Void> future = executor.submit(
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // Get the barrier.
            DoubleBarrier barrier = synchronizationService.getDoubleBarrier(BARRIER_NAME, PARTIES);
            Assert.assertNotNull(barrier);

            // Propagate up the timeout exceptions.
            barrier.enter(10, TimeUnit.SECONDS);
            postEnterLatch.countDown();
            Assert.assertTrue(postEnterLatch.await(1, TimeUnit.SECONDS));

            // Don't let one of the PARTIES exit the barrier.
            if (p != 0) {
              try {
                barrier.leave(10, TimeUnit.SECONDS);
                Assert.fail();
              } catch (TimeoutException ignored) {
              } catch (BrokenBarrierException ignored) {
              }
            }

            return null;
          }
        }
      );

      futures.add(future);
    }

    for (Future<Void> future: futures) {
      future.get(60, TimeUnit.SECONDS);
    }
  }
}
