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

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import org.apache.twill.common.Cancellable;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.NodeService;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
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
public class ZKSynchronizationService implements SynchronizationService, SynchronizationServiceClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZKSynchronizationService.class);
  private static final String NAMESPACE = "/synchronization";

  private final ZKClient zkClient;
  private final NodeService nodeService;

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
    this.nodeService = new NodeService(this.zkClient, CreateMode.PERSISTENT);
  }

  /**
   * Registers a {@link DoubleBarrier} in zookeeper.
   * <p>
   *   Registering a {@link DoubleBarrier} will create a node &lt;base&gt;/&lt;service-name&gt;
   *   in zookeeper as a ephemeral node. If the node already exists (timeout associated with emphemeral node creation), 
   *   then a runtime exception is thrown to make sure that a service with an intent to register is not started without 
   *   registering. 
   *   When a runtime exception is thrown, expectation is that the process being started will fail and would be started 
   *   again by the monitoring service.
   * </p>
   * @param barrierName The name of the barrier.
   * @param parties The number of parties in the barrier.
   * @return An instance of {@link Cancellable}
   */
  @Override
  public Cancellable registerDoubleBarrier(String barrierName, int parties) {
    final String path = "/" + barrierName;
    byte[] data = encode(parties);

    final Cancellable cancellable = nodeService.register(path, data);

    return new Cancellable() {
      @Override
      public void cancel() {
        // Make sure we clean up any broken barriers.

        // TODO: Should we not be doing this? Otherwise we leave behind broken barrier nodes.
        Futures.getUnchecked(ZKOperations.recursiveDelete(zkClient, path));

        cancellable.cancel();
      }
    };
  }

  /**
   * Return an instance of {@link DoubleBarrier}.
   * @param barrierName The name of the barrier.
   * @return An instance of {@link DoubleBarrier}.
   */
  @Override
  public DoubleBarrier getDoubleBarrier(String barrierName) throws ExecutionException, InterruptedException {
    final String barrierBase = "/" + barrierName;

    final SettableFuture<NodeData> nodeDataFuture = SettableFuture.create();

    // Wait until the barrier has been created.
    Cancellable cancellable = ZKOperations.watchData(zkClient, barrierBase, new ZKOperations.DataCallback() {
      @Override
      public void updated(NodeData nodeData) {
        if (!nodeDataFuture.isDone()) {
          nodeDataFuture.set(nodeData);
        }
      }
    });

    NodeData nodeData = nodeDataFuture.get();
    cancellable.cancel();

    int parties = decode(nodeData.getData());
    return new ZKDoubleBarrier(ZKClients.namespace(zkClient, barrierBase), parties);
  }

  private byte[] encode(int parties) {
    return new Gson().toJson(parties).getBytes(Charsets.UTF_8);
  }

  private Integer decode(byte[] encoded) {
    if (encoded == null) {
      return null;
    }
    return new Gson().fromJson(new String(encoded, Charsets.UTF_8), Integer.class);
  }
}

