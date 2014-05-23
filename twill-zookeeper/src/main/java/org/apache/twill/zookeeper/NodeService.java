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
package org.apache.twill.zookeeper;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link NodeService} automates management of creating a node in Zookeeper.
 */
public class NodeService {
  private static final Logger LOG = LoggerFactory.getLogger(NodeService.class);

  private static final long RETRY_MILLIS = 1000;

  // In memory map for recreating ephemeral nodes after session expires.
  // It map from node to the corresponding Cancellable
  private final Multimap<Node, NodeCancellable> nodes;
  private final Lock lock;

  private final ZKClient zkClient;
  private final CreateMode createMode;
  private final ScheduledExecutorService retryExecutor;

  /**
   * Constructs NodeService using the provided zookeeper client for storing service registry.
   * @param zkClient of zookeeper quorum
   */
  public NodeService(ZKClient zkClient, CreateMode createMode) {
    this.zkClient = zkClient;
    this.createMode = createMode;
    this.nodes = HashMultimap.create();
    this.lock = new ReentrantLock();
    this.retryExecutor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("zk-discovery-retry"));
    this.zkClient.addConnectionWatcher(createConnectionWatcher());
  }

  /**
   * Registers a Node in zookeeper.
   * <p>
   *   Registering a Node will create a node &lt;base&gt;/&lt;service-path&gt;
   *   in zookeeper as a ephemeral node. If the node already exists (timeout associated with ephemeral node creation),
   *   then a runtime exception is thrown to make sure that a service with an intent to register is not started without
   *   registering.
   *   When a runtime exception is thrown, expectation is that the process being started will fail and would be started
   *   again by the monitoring service.
   * </p>
   * @param path the path of the node
   * @param data the data contained in the node.
   * @return An instance of {@link Cancellable}
   */
  public Cancellable register(final String path, final byte[] data) {
    final Node node = new Node(path, data);
    final SettableFuture<String> future = SettableFuture.create();
    final NodeCancellable cancellable = new NodeCancellable(node);

    // Create the zk ephemeral node.
    Futures.addCallback(doRegister(node), new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        // Set the sequence node path to cancellable for future cancellation.
        cancellable.setPath(result);
        lock.lock();
        try {
          nodes.put(node, cancellable);
        } finally {
          lock.unlock();
        }
        LOG.debug("Node registered: {} {}", node, result);
        future.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException.NodeExistsException) {
          handleRegisterFailure(node, future, this, t);
        } else {
          LOG.warn("Failed to register: {}", node, t);
          future.setException(t);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Futures.getUnchecked(future);
    return cancellable;
  }

  /**
   * Handle registration failure.
   *
   * @param node The node to register.
   * @param completion A settable future to set when registration is completed / failed.
   * @param creationCallback A future callback for path creation.
   * @param failureCause The original cause of failure.
   */
  private void handleRegisterFailure(final Node node,
                                     final SettableFuture<String> completion,
                                     final FutureCallback<String> creationCallback,
                                     final Throwable failureCause) {

    final String path = node.getPath();
    Futures.addCallback(zkClient.exists(path), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result == null) {
          // If the node is gone, simply retry.
          LOG.info("Node {} is gone. Retry registration for {}.", path, node);
          retryRegister(node, creationCallback);
          return;
        }

        // Make sure we own the node if we're creating an ephemeral node.
        if (createMode.isEphemeral()) {
          long ephemeralOwner = result.getEphemeralOwner();
          if (ephemeralOwner == 0) {
            // it is not an ephemeral node, something wrong.
            LOG.error("Node {} already exists and is not an ephemeral node. Node registration failed: {}.",
              path, node);
            completion.setException(failureCause);
            return;
          }
          Long sessionId = zkClient.getSessionId();
          if (sessionId == null || ephemeralOwner != sessionId) {
            // This zkClient is not valid or doesn't own the ephemeral node, simply keep retrying.
            LOG.info("Owner of {} is different. Retry registration for {}.", path, node);
            retryRegister(node, creationCallback);
          } else {
            // This client owned the node, treat the registration as completed.
            // This could happen if same client tries to register twice (due to mistake or failure race condition).
            completion.set(path);
          }
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // If exists call failed, simply retry creation.
        LOG.warn("Error when getting stats on {}. Retry registration for {}.", path, node);
        retryRegister(node, creationCallback);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private OperationFuture<String> doRegister(Node node) {
    return zkClient.create(node.getPath(), node.getData(), createMode, true);
  }

  private void retryRegister(final Node node, final FutureCallback<String> creationCallback) {
    retryExecutor.schedule(new Runnable() {

      @Override
      public void run() {
        Futures.addCallback(doRegister(node), creationCallback, Threads.SAME_THREAD_EXECUTOR);
      }
    }, RETRY_MILLIS, TimeUnit.MILLISECONDS);
  }

  private static class Node {
    private final String path;
    private final byte[] data;

    public Node(String path, byte[] data) {
      this.path = path;
      this.data = data;
    }

    public String getPath() {
      return path;
    }

    public byte[] getData() {
      return data;
    }
  }

  private Watcher createConnectionWatcher() {
    return new Watcher() {
      // Watcher is invoked from single event thread, hence safe to use normal mutable variable.
      private boolean expired;

      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.Expired) {
          LOG.warn("ZK Session expired: {}", zkClient.getConnectString());
          expired = true;
        } else if (event.getState() == Event.KeeperState.SyncConnected && expired) {
          LOG.info("Reconnected after expiration: {}", zkClient.getConnectString());
          expired = false;

          // Re-register all nodes
          lock.lock();
          try {
            for (final Map.Entry<Node, NodeCancellable> entry : nodes.entries()) {
              LOG.info("Re-registering service: {}", entry.getKey());

              // Must be non-blocking in here.
              Futures.addCallback(doRegister(entry.getKey()), new FutureCallback<String>() {
                @Override
                public void onSuccess(String result) {
                  // Updates the cancellable to the newly created sequential node.
                  entry.getValue().setPath(result);
                  LOG.debug("Service re-registered: {} {}", entry.getKey(), result);
                }

                @Override
                public void onFailure(Throwable t) {
                  // When failed to create the node, there would be no retry and simply make the cancellable do nothing.
                  entry.getValue().setPath(null);
                  LOG.error("Failed to re-register service: {}", entry.getKey(), t);
                }
              }, Threads.SAME_THREAD_EXECUTOR);
            }
          } finally {
            lock.unlock();
          }
        }
      }
    };
  }

  /**
   * Inner class for cancelling (un-register) a node.
   */
  private final class NodeCancellable implements Cancellable {

    private final Node node;
    private final AtomicBoolean cancelled;
    private volatile String path;

    NodeCancellable(Node node) {
      this.node = node;
      this.cancelled = new AtomicBoolean();
    }

    /**
     * Set the zk node path representing the ephemeral sequence node of this registered node.
     * Called from ZK event thread when creating of the node completed, either from normal registration or
     * re-registration due to session expiration.
     *
     * @param path The path to ephemeral sequence node.
     */
    void setPath(String path) {
      this.path = path;
      if (cancelled.get() && path != null) {
        // Simply delete the path if it's already cancelled
        // It's for the case when session expire happened and re-registration completed after this has been cancelled.
        // Not bother with the result as if there is error, nothing much we could do.
        zkClient.delete(path);
      }
    }

    @Override
    public void cancel() {
      if (!cancelled.compareAndSet(false, true)) {
        return;
      }

      // Take a snapshot of the volatile path.
      String path = this.path;

      // If it is null, meaning cancel() is called before the ephemeral node is created, hence
      // setPath() will be called in future (through zk callback when creation is completed)
      // so that deletion will be done in setPath().
      if (path == null) {
        return;
      }

      // Remove this Cancellable from the map so that upon session expiration won't try to register.
      lock.lock();
      try {
        nodes.remove(node, this);
      } finally {
        lock.unlock();
      }

      // Delete the path. It's ok if the path not exists
      // (e.g. what session expired and before node has been re-created)
      Futures.getUnchecked(ZKOperations.ignoreError(zkClient.delete(path),
                                                    KeeperException.NoNodeException.class, path));
      LOG.debug("Node unregistered: {} {}", node, path);
    }
  }
}
