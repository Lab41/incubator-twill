package org.apache.twill.synchronization;

import org.apache.twill.common.Cancellable;

/**
 * tbd
 */
public interface SynchronizationService {

  /**
   * Registers a {@link DoubleBarrier}.
   * @param barrierName The name of the barrier.
   * @param parties The number of parties in the barrier.
   * @return A {@link Cancellable} for un-registration.
   */
  public Cancellable registerDoubleBarrier(String barrierName, int parties);
}
