/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.yarn;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Services;
import org.apache.twill.synchronization.DoubleBarrier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for ability to use double barriers {@link org.apache.twill.api.TwillContext}.
 */
public final class DoubleBarrierTest extends BaseYarnTest {

  private static final Logger LOG = LoggerFactory.getLogger(DoubleBarrierTest.class);

  private static final String BARRIER_NAME = "barrier";
  private static final int PARTIES = 2;

  @Test
  public void testDoubleBarrier() throws InterruptedException, ExecutionException, TimeoutException {
    TwillRunner twillRunner = YarnTestUtils.getTwillRunner();

    TwillController controller = twillRunner
      .prepare(new DoubleBarrierApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    ListenableFuture<Service.State> completion = Services.getCompletionFuture(controller);
    try {
      completion.get(60, TimeUnit.SECONDS);
    } finally {
      controller.stopAndWait();
    }
  }

  /**
   * An application that contains two {@link DoubleBarrierRunnable}.
   */
  public static final class DoubleBarrierApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("DoubleBarrierApp")
        .withRunnable()
          .add(new DoubleBarrierRunnable()).noLocalFiles()
          .add(new DoubleBarrierRunnable()).noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  /**
   * A Runnable that will announce on service and wait for announcement from another instance in the same service.
   */
  public static final class DoubleBarrierRunnable extends AbstractTwillRunnable {

    @Override
    public void run() {
      try {
        DoubleBarrier barrier = getContext().getDoubleBarrier(BARRIER_NAME, PARTIES);
        LOG.info("entering barrier");
        barrier.enter();
        LOG.info("leaving barrier");
        barrier.leave();
        LOG.info("left barrier");
      } catch (Exception e) {
        Assert.fail("unexpected exception");
      }
    }

    @Override
    public void stop() {
      Thread.currentThread().interrupt();
    }
  }
}
