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
