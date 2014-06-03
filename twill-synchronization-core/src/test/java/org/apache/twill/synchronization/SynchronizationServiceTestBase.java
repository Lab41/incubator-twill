package org.apache.twill.synchronization;

import com.google.common.collect.Lists;
import org.apache.twill.common.Cancellable;
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

  protected abstract Map.Entry<SynchronizationService, SynchronizationServiceClient> create();

  @Test
  public void testMultiplePartiesEnterAndLeave() throws Exception {
    Map.Entry<SynchronizationService, SynchronizationServiceClient> entry = create();
    SynchronizationService synchronizationService = entry.getKey();
    final SynchronizationServiceClient synchronizationServiceClient = entry.getValue();

    // Register a barrier.
    int parties = 3;
    Cancellable cancellable = synchronizationService.registerDoubleBarrier(BARRIER_NAME, parties);

    ExecutorService executor = Executors.newFixedThreadPool(parties);
    List<Future<Void>> futures = Lists.newArrayList();

    final CountDownLatch postEnterLatch = new CountDownLatch(parties);
    final CountDownLatch postLeaveLatch = new CountDownLatch(parties);

    for (int party = 0; party < parties; ++party) {
      Future<Void> future = executor.submit(
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // Get the barrier.
            DoubleBarrier barrier = synchronizationServiceClient.getDoubleBarrier(BARRIER_NAME);
            Assert.assertNotNull(barrier);

            // Propagate up the timeout exceptions.
            barrier.enter(1, TimeUnit.SECONDS);
            postEnterLatch.countDown();
            Assert.assertTrue(postEnterLatch.await(1, TimeUnit.SECONDS));

            // Propagate up the timeout exceptions.
            barrier.leave(1, TimeUnit.SECONDS);
            postLeaveLatch.countDown();
            Assert.assertTrue(postLeaveLatch.await(1, TimeUnit.SECONDS));

            return null;
          }
        }
      );
      futures.add(future);
    }

    for (Future<Void> future: futures) {
      future.get(); //1, TimeUnit.SECONDS);
    }

    // Remove the barrier.
    cancellable.cancel();
  }

  @Test
  public void testFailingToEnterBarrier() throws Exception {
    Map.Entry<SynchronizationService, SynchronizationServiceClient> entry = create();
    SynchronizationService synchronizationService = entry.getKey();
    final SynchronizationServiceClient synchronizationServiceClient = entry.getValue();

    // Register a barrier.
    int parties = 3;
    Cancellable cancellable = synchronizationService.registerDoubleBarrier(BARRIER_NAME, parties);

    ExecutorService executor = Executors.newFixedThreadPool(parties);
    List<Future<Void>> futures = Lists.newArrayList();

    for (int party = 0; party < parties - 1; ++party) {
      Future<Void> future = executor.submit(
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // Get the barrier.
            DoubleBarrier barrier = synchronizationServiceClient.getDoubleBarrier(BARRIER_NAME);
            Assert.assertNotNull(barrier);

            // Propagate up the timeout exceptions.

            try {
              barrier.enter(1, TimeUnit.SECONDS);
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
      future.get(5, TimeUnit.SECONDS);
    }

    // Remove the barrier.
    cancellable.cancel();
  }

  @Test
  public void testFailingToLeaveBarrier() throws Exception {
    Map.Entry<SynchronizationService, SynchronizationServiceClient> entry = create();
    SynchronizationService synchronizationService = entry.getKey();
    final SynchronizationServiceClient synchronizationServiceClient = entry.getValue();

    // Register a barrier.
    int parties = 3;
    Cancellable cancellable = synchronizationService.registerDoubleBarrier(BARRIER_NAME, parties);

    ExecutorService executor = Executors.newFixedThreadPool(parties);
    List<Future<Void>> futures = Lists.newArrayList();

    final CountDownLatch postEnterLatch = new CountDownLatch(parties);

    for (int party = 0; party < parties; ++party) {
      final int p = party;

      Future<Void> future = executor.submit(
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // Get the barrier.
            DoubleBarrier barrier = synchronizationServiceClient.getDoubleBarrier(BARRIER_NAME);
            Assert.assertNotNull(barrier);

            // Propagate up the timeout exceptions.
            barrier.enter(1, TimeUnit.SECONDS);
            postEnterLatch.countDown();
            Assert.assertTrue(postEnterLatch.await(1, TimeUnit.SECONDS));

            // Don't let one of the parties exit the barrier.
            if (p != 0) {
              try {
                barrier.leave(1, TimeUnit.SECONDS);
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
      future.get(5, TimeUnit.SECONDS);
    }

    // Remove the barrier.
    cancellable.cancel();
  }
}
