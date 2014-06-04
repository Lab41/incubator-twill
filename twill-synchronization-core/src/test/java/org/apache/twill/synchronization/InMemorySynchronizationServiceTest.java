package org.apache.twill.synchronization;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Test memory based synchronization service.
 */
public class InMemorySynchronizationServiceTest extends SynchronizationServiceTestBase {

  @Override
  protected SynchronizationService create() {
    return new InMemorySynchronizationService();
  }
}
