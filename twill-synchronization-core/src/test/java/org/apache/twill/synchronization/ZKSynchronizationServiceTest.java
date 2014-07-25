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

import com.google.common.util.concurrent.Futures;
import org.apache.twill.common.Services;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

/**
 * Test Zookeeper synchronization service.
 */
public class ZKSynchronizationServiceTest extends SynchronizationServiceTestBase {

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClient;

  @Before
  public void beforeClass() {
    zkServer = InMemoryZKServer.builder().setTickTime(100000).build();
    zkServer.startAndWait();

    zkClient = ZKClientServices.delegate(
      ZKClients.retryOnFailure(
        ZKClients.reWatchOnExpire(
          ZKClientService.Builder.of(zkServer.getConnectionStr()).build()),
        RetryStrategies.fixDelay(1, TimeUnit.SECONDS)));
    zkClient.startAndWait();
  }

  @After
  public void afterClass() {
    Futures.getUnchecked(Services.chainStop(zkClient, zkServer));
  }

  @Override
  protected SynchronizationService create() {
    return new ZKSynchronizationService(zkClient);
  }
}
