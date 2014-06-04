package org.apache.twill.synchronization;

/**
 * tbd
 */
public interface SynchronizationService {

  /**
   * Return an instance of {@link DoubleBarrier}.
   * @param barrierName The name of the barrier.
   * @param parties The minimum number of members in the party before it is entered.
   * @return An instance of {@link DoubleBarrier}.
   */
  DoubleBarrier getDoubleBarrier(String barrierName, int parties) throws Exception;
}
