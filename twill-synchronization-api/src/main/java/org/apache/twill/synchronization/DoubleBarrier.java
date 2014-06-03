package org.apache.twill.synchronization;

import java.util.concurrent.TimeUnit;

/**
 * TBD.
 */
public interface DoubleBarrier {

  /**
   * @return the number of parties in the barrier.
   */
  public int getParties();

  /**
   * Enter the barrier and block until all members have entered.
   *
   * @throws Exception
   */
  public void enter() throws Exception;

  /**
   * Enter the barrier and block until all members have entered or the timeout has expired.
   *
   * @param maxWait max time to block
   * @param unit time unit
   * @throws java.util.concurrent.TimeoutException if the specified timeout elapses.
   */
  public void enter(long maxWait, TimeUnit unit) throws Exception;

  public void leave() throws Exception;

  /**
   * Leave the barrier and block until all members have entered or the timeout has expired.
   *
   * @param maxWait max time to block
   * @param unit time unit
   * @throws java.util.concurrent.TimeoutException if the specified timeout elapses.
   */
  public void leave(long maxWait, TimeUnit unit) throws Exception;
}
