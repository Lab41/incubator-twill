package org.apache.twill.synchronization;

/**
 * tbd.
 */
public interface SynchronizationServiceClient {

  /**
   * Return an instance of {@link DoubleBarrier}.
   * @param barrierName The name of the barrier.
   * @return An instance of {@link DoubleBarrier}.
   */
  DoubleBarrier getDoubleBarrier(String barrierName) throws Exception;
}
