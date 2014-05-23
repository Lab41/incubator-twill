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
package org.apache.twill.discovery;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.NodeService;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Zookeeper implementation of {@link DiscoveryService} and {@link DiscoveryServiceClient}.
 * <p>
 *   Discoverable services are registered within Zookeeper under the namespace 'discoverable' by default.
 *   If you would like to change the namespace under which the services are registered then you can pass
 *   in the namespace during construction of {@link ZKDiscoveryService}.
 * </p>
 *
 * <p>
 *   Following is a simple example of how {@link ZKDiscoveryService} can be used for registering services
 *   and also for discovering the registered services.
 * </p>
 *
 * <blockquote>
 *   <pre>
 *     {@code
 *
 *     DiscoveryService service = new ZKDiscoveryService(zkClient);
 *     service.register(new Discoverable() {
 *       &#64;Override
 *       public String getName() {
 *         return 'service-name';
 *       }
 *
 *       &#64;Override
 *       public InetSocketAddress getSocketAddress() {
 *         return new InetSocketAddress(hostname, port);
 *       }
 *     });
 *     ...
 *     ...
 *     ServiceDiscovered services = service.discovery("service-name");
 *     ...
 *     }
 *   </pre>
 * </blockquote>
 */
public class ZKDiscoveryService implements DiscoveryService, DiscoveryServiceClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZKDiscoveryService.class);
  private static final String NAMESPACE = "/discoverable";

  private final LoadingCache<String, ServiceDiscovered> services;
  private final ZKClient zkClient;
  private final NodeService nodeService;

  /**
   * Constructs ZKDiscoveryService using the provided zookeeper client for storing service registry.
   * @param zkClient The {@link ZKClient} for interacting with zookeeper.
   */
  public ZKDiscoveryService(ZKClient zkClient) {
    this(zkClient, NAMESPACE);
  }

  /**
   * Constructs ZKDiscoveryService using the provided zookeeper client for storing service registry under namespace.
   * @param zkClient of zookeeper quorum
   * @param namespace under which the service registered would be stored in zookeeper.
   *                  If namespace is {@code null}, no namespace will be used.
   */
  public ZKDiscoveryService(ZKClient zkClient, String namespace) {
    this.zkClient = namespace == null ? zkClient : ZKClients.namespace(zkClient, namespace);
    this.nodeService = new NodeService(this.zkClient, CreateMode.EPHEMERAL);
    this.services = CacheBuilder.newBuilder().build(createServiceLoader());
  }

  /**
   * Registers a {@link Discoverable} in zookeeper.
   * <p>
   *   Registering a {@link Discoverable} will create a node &lt;base&gt;/&lt;service-name&gt;
   *   in zookeeper as a ephemeral node. If the node already exists (timeout associated with emphemeral node creation), 
   *   then a runtime exception is thrown to make sure that a service with an intent to register is not started without 
   *   registering. 
   *   When a runtime exception is thrown, expectation is that the process being started will fail and would be started 
   *   again by the monitoring service.
   * </p>
   * @param discoverable Information of the service provider that could be discovered.
   * @return An instance of {@link Cancellable}
   */
  @Override
  public Cancellable register(final Discoverable discoverable) {
    final Discoverable wrapper = new DiscoverableWrapper(discoverable);

    String path = getNodePath(wrapper);
    byte[] data = DiscoverableAdapter.encode(discoverable);

    return nodeService.register(path, data);
  }

  @Override
  public ServiceDiscovered discover(String service) {
    return services.getUnchecked(service);
  }

  /**
   * Generate unique node path for a given {@link Discoverable}.
   * @param discoverable An instance of {@link Discoverable}.
   * @return A node name based on the discoverable.
   */
  private String getNodePath(Discoverable discoverable) {
    InetSocketAddress socketAddress = discoverable.getSocketAddress();
    String node = Hashing.md5()
                         .newHasher()
                         .putBytes(socketAddress.getAddress().getAddress())
                         .putInt(socketAddress.getPort())
                         .hash().toString();

    return String.format("/%s/%s", discoverable.getName(), node);
  }

  /**
   * Creates a CacheLoader for creating live Iterable for watching instances changes for a given service.
   */
  private CacheLoader<String, ServiceDiscovered> createServiceLoader() {
    return new CacheLoader<String, ServiceDiscovered>() {
      @Override
      public ServiceDiscovered load(String service) throws Exception {
        final DefaultServiceDiscovered serviceDiscovered = new DefaultServiceDiscovered(service);
        final String serviceBase = "/" + service;

        // Watch for children changes in /service
        ZKOperations.watchChildren(zkClient, serviceBase, new ZKOperations.ChildrenCallback() {
          @Override
          public void updated(NodeChildren nodeChildren) {
            // Fetch data of all children nodes in parallel.
            List<String> children = nodeChildren.getChildren();
            List<OperationFuture<NodeData>> dataFutures = Lists.newArrayListWithCapacity(children.size());
            for (String child : children) {
              dataFutures.add(zkClient.getData(serviceBase + "/" + child));
            }

            // Update the service map when all fetching are done.
            final ListenableFuture<List<NodeData>> fetchFuture = Futures.successfulAsList(dataFutures);
            fetchFuture.addListener(new Runnable() {
              @Override
              public void run() {
                ImmutableSet.Builder<Discoverable> builder = ImmutableSet.builder();
                for (NodeData nodeData : Futures.getUnchecked(fetchFuture)) {
                  // For successful fetch, decode the content.
                  if (nodeData != null) {
                    Discoverable discoverable = DiscoverableAdapter.decode(nodeData.getData());
                    if (discoverable != null) {
                      builder.add(discoverable);
                    }
                  }
                }
                serviceDiscovered.setDiscoverables(builder.build());
              }
            }, Threads.SAME_THREAD_EXECUTOR);
          }
        });
        return serviceDiscovered;
      }
    };
  }

}

