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
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * Zookeeper implementation of {@link SynchronizationService} and {@link SynchronizationServiceClient}.
 * <p>
 *   Synchronization primitives are registered within Zookeeper under the namespace 'synchronization' by default.
 *   If you would like to change the namespace under which the services are registered then you can pass
 *   in the namespace during the construction of {@link ZKSynchronizationService}.
 * </p>
 *
 * <p>
 *   Follwing is a simple example of how {@link ZKSynchronizationService} can be used for registering synchronization
 *   primitives and also using those primitives.
 * </p>
 * <blockquote>
 *   <pre>
 *     {@code
 *
 *     SynchronizationService service = new ZKSynchronizationService(zkClient);
 *     service.registerDoubleBarrier("barrier-name", 1);
 *     ...
 *     ...
 *     Barrier barrier = service.getDoubleBarrier("barrier-name");
 *     barrier.enter();
 *     ...
 *     }
 *   </pre>
 * </blockquote>
 */
public class ZKSynchronizationService implements SynchronizationService {
  private static final Logger LOG = LoggerFactory.getLogger(ZKSynchronizationService.class);
  private static final String NAMESPACE = "/synchronization";

  private final ZKClient zkClient;

  /**
   * Constructs ZKSynchronizationService using the provided zookeeper client for storing primitives.
   *
   * @param zkClient The {@link ZKClient} for interacting with zookeeper.
   */
  public ZKSynchronizationService(ZKClient zkClient) {
    this(zkClient, NAMESPACE);
  }

  /**
   * Constructs ZKSynchronizationService using the provided zookeeper client for storing primitives under namespace.
   * @param zkClient of zookeeper quorum
   * @param namespace under which the primitives registered would be stored in zookeeper.
   *                  If namespace is {@code null}, no namespace will be used.
   */
  public ZKSynchronizationService(ZKClient zkClient, String namespace) {
    this.zkClient = namespace == null ? zkClient : ZKClients.namespace(zkClient, namespace);
  }

  /**
   * Return an instance of {@link DoubleBarrier}.
   * @param name The name of the barrier.
   * @param parties the number of parties waiting at the barrier before it is entered.
   * @return An instance of {@link DoubleBarrier}.
   */
  @Override
  public DoubleBarrier getDoubleBarrier(String name, int parties) throws ExecutionException, InterruptedException {
    final String barrierBase = "/" + name;

    LOG.debug("Getting barrier", barrierBase);

    // Make sure the barrier has been created.
    Futures.getUnchecked(ZKOperations.ignoreError(zkClient.create(barrierBase, null, CreateMode.PERSISTENT, true),
                                                  KeeperException.NodeExistsException.class,
                                                  null));

    return new ZKDoubleBarrier(ZKClients.namespace(zkClient, barrierBase), parties);
  }
}

