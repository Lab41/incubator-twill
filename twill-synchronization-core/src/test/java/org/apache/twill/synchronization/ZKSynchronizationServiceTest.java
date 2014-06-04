package org.apache.twill.synchronization;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import org.apache.twill.common.Services;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Map;
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
