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
package org.apache.twill.api;

import com.google.common.collect.Iterables;
import org.apache.twill.internal.DefaultResourceSpecification;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * This interface provides specifications for resource requirements including set and get methods
 * for number of cores, amount of memory, and number of instances.
 */
public interface ResourceSpecification {

  final ResourceSpecification BASIC = Builder.with().setVirtualCores(1).setMemory(512, SizeUnit.MEGA).build();

  /**
   * Unit for specifying memory size.
   */
  enum SizeUnit {
    MEGA(1),
    GIGA(1024);

    private final int multiplier;

    private SizeUnit(int multiplier) {
      this.multiplier = multiplier;
    }
  }

  /**
   * Returns the number of virtual CPU cores. DEPRECATED, use getVirtualCores instead.
   * @return Number of virtual CPU cores.
   */
  @Deprecated
  int getCores();

  /**
   * Returns the number of virtual CPU cores.
   * @return Number of virtual CPU cores.
   */
  int getVirtualCores();

  /**
   * Returns the memory size in MB.
   * @return Memory size
   */
  int getMemorySize();

  /**
   * Returns the uplink bandwidth in Mbps.
   * @return Uplink bandwidth or -1 representing unlimited bandwidth.
   */
  int getUplink();

  /**
   * Returns the downlink bandwidth in Mbps.
   * @return Downlink bandwidth or -1 representing unlimited bandwidth.
   */
  int getDownlink();

  /**
   * Returns number of execution instances.
   * @return Number of execution instances.
   */
  int getInstances();

  /**
   * Returns the execution hosts, expects Fully Qualified Domain Names host + domain.
   * This is a suggestion for the scheduler depending on cluster load it may ignore it
   * @return An array containing the hosts where the containers should run
   */
  List<String> getHosts();

  /**
   * Returns the execution racks.
   * This is a suggestion for the scheduler depending on cluster load it may ignore it
   * @return An array containing the racks where the containers should run
   */
  List<String> getRacks();

  /**
   * Builder for creating {@link ResourceSpecification}.
   */
  static final class Builder {

    private int cores;
    private int memory;
    private int uplink = -1;
    private int downlink = -1;
    private int instances = 1;
    private List<String> hosts = new LinkedList<String>();
    private List<String> racks = new LinkedList<String>();

    public static CoreSetter with() {
      return new Builder().new CoreSetter();
    }

    public final class CoreSetter {
      @Deprecated
      public MemorySetter setCores(int cores) {
        Builder.this.cores = cores;
        return new MemorySetter();
      }

      public MemorySetter setVirtualCores(int cores) {
        Builder.this.cores = cores;
        return new MemorySetter();
      }
    }

    public final class MemorySetter {
      public AfterMemory setMemory(int size, SizeUnit unit) {
        Builder.this.memory = size * unit.multiplier;
        return new AfterMemory();
      }
    }

    public final class AfterMemory extends Build {
      public AfterInstances setInstances(int instances) {
        Builder.this.instances = instances;
        return new AfterInstances();
      }
    }

    public final class AfterInstances extends Build {
      public AfterUplink setUplink(int uplink, SizeUnit unit) {
        Builder.this.uplink = uplink * unit.multiplier;
        return new AfterUplink();
      }
    }

    public final class AfterUplink extends Build {
      public AfterDownlink setDownlink(int downlink, SizeUnit unit) {
        Builder.this.downlink = downlink * unit.multiplier;
        return new AfterDownlink();
      }
    }

    public final class AfterHosts extends Build {
      public Done setRacks(String... racks) {
        if (racks != null) {
          Builder.this.racks = Arrays.asList(racks);
        }
        return new Done();
      }

      public Done setRacks(Iterable<String> racks) {
        if (racks != null) {
          Iterables.addAll(Builder.this.racks, racks);
        }
        return new Done();
      }
    }

    public final class AfterDownlink extends Build {
      public AfterHosts setHosts(String... hosts) {
        if (hosts != null) {
          Builder.this.hosts = Arrays.asList(hosts);
        }
        return new AfterHosts();
      }

      public AfterHosts setHosts(Iterable<String> hosts) {
        if (hosts != null) {
          Iterables.addAll(Builder.this.hosts, hosts);
        }
        return new AfterHosts();
      }
    }

    public final class Done extends Build {
    }

    public abstract class Build {
      public ResourceSpecification build() {
        return new DefaultResourceSpecification(cores, memory, instances, uplink, downlink, hosts, racks);
      }
    }

    private Builder() {}
  }
}
