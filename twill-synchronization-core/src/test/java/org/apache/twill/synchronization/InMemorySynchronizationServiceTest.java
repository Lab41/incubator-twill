package org.apache.twill.synchronization;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Test memory based synchronization service.
 */
public class InMemorySynchronizationServiceTest extends SynchronizationServiceTestBase {

  @Override
  protected Map.Entry<SynchronizationService, SynchronizationServiceClient> create() {
    SynchronizationService synchronizationService = new InMemorySynchronizationService();
    return Maps.immutableEntry(synchronizationService, (SynchronizationServiceClient) synchronizationService);
  }
}
