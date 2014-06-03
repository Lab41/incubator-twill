/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.twill.synchronization;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A double barrier as described in the Zookeeper recipes. Greatly inspired by Apache Curator's implementation of double
 * barriers.
 */
final class ZKDoubleBarrier implements DoubleBarrier {

  private static final String READY_NODE = "ready";

  private final ZKClient zkClient;
  private final int parties;
  private final String ourPath;
  private final String readyPath;

  public ZKDoubleBarrier(ZKClient zkClient, int parties) {
    this.zkClient = zkClient;
    this.parties = parties;
    this.ourPath = UUID.randomUUID().toString();
    this.readyPath = READY_NODE;
  }

  @Override
  public int getParties() {
    return parties;
  }

  @Override
  public void enter() throws Exception {
    enter(-1, null);
  }

  @Override
  public void enter(long maxWait, TimeUnit unit) throws Exception {

    // This implements the entering of a double barrier algorithm as expressed in
    // http://zookeeper.apache.org/doc/trunk/recipes.html#sc_doubleBarriers.

    // TODO: do I have to care about connection loss?

    String ourBase = "/" + ourPath;
    String readyBase = "/" + readyPath;

    // Step 2: watch for the /ready path to be created.
    ListenableFuture<String> existsFuture = ZKOperations.watchExists(zkClient, readyPath);

    // Step 3: Create our process node.
    zkClient.create(ourBase, null, CreateMode.EPHEMERAL, true).get();

    // Step 4: Get all the children.
    NodeChildren nodeChildren = zkClient.getChildren("").get();
    List<String> children = filterAndSortChildren(nodeChildren.getChildren());

    int count = (children != null) ? children.size() : 0;
    if (count < parties) {
      // Step 5: If not enough processes are in the barrier, wait for the ready node to be created.
      existsFuture.get(maxWait, unit);
    } else {
      // Step 6: Create the ready node.
      ZKOperations.ignoreError(
        zkClient.create(readyBase, null, CreateMode.EPHEMERAL, true),
        KeeperException.NodeExistsException.class,
        readyBase);
    }
  }

  @Override
  public void leave() throws Exception {
    leave(-1, null);
  }

  @Override
  public void leave(long maxWait, TimeUnit unit) throws Exception {

    // This implements the leaving of a double barrier algorithm as expressed in
    // http://zookeeper.apache.org/doc/trunk/recipes.html#sc_doubleBarriers.

    // TODO: do I have to care about connection loss?
    long startMs = System.currentTimeMillis();
    boolean hasMaxWait = unit != null;
    long maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

    String ourBase = "/" + ourPath;
    String readyBase = "/" + readyPath;

    boolean ourNodeShouldExist = true;

    while (true) {
      // Step 1: Get all the children.
      NodeChildren nodeChildren = zkClient.getChildren("").get();
      List<String> children = filterAndSortChildren(nodeChildren.getChildren());

      // Step 2: Exit early if there are no nodes left.
      int count = (children != null) ? children.size() : 0;
      if (count == 0) {
        break;
      }

      // Handle the bad state where we're not in the list but we thought we should have been.
      int ourIndex = children.indexOf(ourPath);
      if (ourNodeShouldExist && ourIndex < 0) {
        throw new IllegalStateException(String.format("Our path (%s) is missing", ourPath));
      }

      // Step 3: If we are the only process in the list, delete and exit.
      if (count == 1) {
        String lastPath = children.get(0);
        if (ourNodeShouldExist && !lastPath.equals(ourPath)) {
          throw new IllegalStateException(String.format("Last path (%s) is not ours (%s)", lastPath, ourPath));
        }

        // We're the last path, so delete ourselves and break out of the loop.
        ZKOperations.ignoreError(zkClient.delete(ourBase), KeeperException.NoNodeException.class, null).get();
        break;
      }

      String path;
      boolean isLowestNode = ourIndex == 0;
      if (isLowestNode) {
        // Step 4: If we are the lowest node in the list, wait on the highest process in the list.

        path = children.get(count - 1);
      } else {
        // Step 5: Delete ourselves if it exists and wait on the lowest process in the list.

        path = children.get(0);

        // Delete our path.
        ZKOperations.ignoreError(zkClient.delete(ourBase), KeeperException.NoNodeException.class, null).get();
        ourNodeShouldExist = false;
      }

      if (hasMaxWait) {
        long elapsed = System.currentTimeMillis() - startMs;
        long thisWaitMs = maxWaitMs - elapsed;
        if (thisWaitMs <= 0) {
          throw new TimeoutException();
        } else {
          ZKOperations.watchDeleted(zkClient, path).get(elapsed, TimeUnit.MILLISECONDS);
        }
      } else {
        Futures.getUnchecked(ZKOperations.watchDeleted(zkClient, path));
      }

      // Step 6: goto step 1.
    }

    // Finally, delete the ready node.
    Futures.getUnchecked(ZKOperations.ignoreError(zkClient.delete(readyBase),
      KeeperException.NoNodeException.class,
      null));
  }

  private List<String> filterAndSortChildren(List<String> children) {
    Iterable<String> filtered = Iterables.filter(children, new Predicate<String>() {
      @Override
      public boolean apply(@Nullable String name) {
        return name == null || !name.equals(READY_NODE);
      }
    });

    ArrayList<String> filteredList = Lists.newArrayList(filtered);
    Collections.sort(filteredList);
    return filteredList;
  }
}
