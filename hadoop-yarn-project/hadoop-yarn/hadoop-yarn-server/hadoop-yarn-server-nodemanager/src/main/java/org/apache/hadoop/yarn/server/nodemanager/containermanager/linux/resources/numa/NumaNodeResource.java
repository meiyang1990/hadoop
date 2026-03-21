// 这个文件已经全部加上中文注释
/**
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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * 存储单个NUMA节点的拓扑信息与资源使用情况，负责管理该节点上内存和CPU资源的分配与释放，为YARN NUMA感知调度提供资源状态管理。
 */
public class NumaNodeResource {
  private String nodeId;
  private long totalMemory;
  private int totalCpus;
  private long usedMemory;
  private int usedCpus;

  private static final Logger LOG = LoggerFactory.
      getLogger(NumaNodeResource.class);

  // 记录每个容器在本NUMA节点占用的内存
  private Map<ContainerId, Long> containerVsMemUsage =
      new ConcurrentHashMap<>();
  // 记录每个容器在本NUMA节点占用的CPU核数
  private Map<ContainerId, Integer> containerVsCpusUsage =
      new ConcurrentHashMap<>();

  /**
   * 构造NUMA节点资源对象，初始化节点总资源容量。
   * @param nodeId NUMA节点ID
   * @param totalMemory 该节点总内存容量(MB)
   * @param totalCpus 该节点总CPU核数
   */
  public NumaNodeResource(String nodeId, long totalMemory, int totalCpus) {
    this.nodeId = nodeId;
    this.totalMemory = totalMemory;
    this.totalCpus = totalCpus;
  }

  /**
   * 检查当前NUMA节点是否有足够的可用资源满足容器请求。
   *
   * @param resource 容器请求的资源量
   * @return true表示资源足够可分配，false表示资源不足
   */
  public boolean isResourcesAvailable(Resource resource) {
    // 调试日志：输出当前可用资源与请求资源
    LOG.debug(
        "Memory available:" + (totalMemory - usedMemory) + ", CPUs available:"
            + (totalCpus - usedCpus) + ", requested:" + resource);
    if ((totalMemory - usedMemory) >= resource.getMemorySize()
        && (totalCpus - usedCpus) >= resource.getVirtualCores()) {
      return true;
    }
    return false;
  }

  /**
   * 为容器分配本节点可用内存，若本节点内存不足则分配全部可用内存，返回剩余需要的内存量。
   *
   * @param memreq 容器需要的内存总量
   * @param containerId 目标容器ID
   * @return 本节点无法满足的剩余内存需求量，0表示完全满足
   */
  public long assignAvailableMemory(long memreq, ContainerId containerId) {
    long memAvailable = totalMemory - usedMemory;
    if (memAvailable >= memreq) {
      containerVsMemUsage.put(containerId, memreq);
      usedMemory += memreq;
      return 0;
    } else {
      usedMemory += memAvailable;
      containerVsMemUsage.put(containerId, memAvailable);
      return memreq - memAvailable;
    }
  }

  /**
   * 为容器分配本节点可用CPU，若本节点CPU不足则分配全部可用CPU，返回剩余需要的CPU数量。
   *
   * @param cpusreq 容器需要的CPU总核数
   * @param containerId 目标容器ID
   * @return 本节点无法满足的剩余CPU需求量，0表示完全满足
   */
  public int assignAvailableCpus(int cpusreq, ContainerId containerId) {
    int cpusAvailable = totalCpus - usedCpus;
    if (cpusAvailable >= cpusreq) {
      containerVsCpusUsage.put(containerId, cpusreq);
      usedCpus += cpusreq;
      return 0;
    } else {
      usedCpus += cpusAvailable;
      containerVsCpusUsage.put(containerId, cpusAvailable);
      return cpusreq - cpusAvailable;
    }
  }

  /**
   * 直接为容器分配全部请求的资源，假定资源已经过可用性检查，不会检查容量。
   *
   * @param resource 请求分配的资源量
   * @param containerId 目标容器ID
   */
  public void assignResources(Resource resource, ContainerId containerId) {
    containerVsMemUsage.put(containerId, resource.getMemorySize());
    containerVsCpusUsage.put(containerId, resource.getVirtualCores());
    usedMemory += resource.getMemorySize();
    usedCpus += resource.getVirtualCores();
  }

  /**
   * 释放容器占用的本节点资源，回收已使用的内存和CPU。
   *
   * @param containerId 需要释放资源的容器ID
   */
  public void releaseResources(ContainerId containerId) {
    if (containerVsMemUsage.containsKey(containerId)) {
      usedMemory -= containerVsMemUsage.get(containerId);
      containerVsMemUsage.remove(containerId);
    }
    if (containerVsCpusUsage.containsKey(containerId)) {
      usedCpus -= containerVsCpusUsage.get(containerId);
      containerVsCpusUsage.remove(containerId);
    }
  }

  /**
   * 恢复容器占用的内存资源，用于NM重启后恢复已有容器的资源分配状态。
   *
   * @param containerId 需要恢复的容器ID
   * @param memory 容器占用的内存量
   */
  public void recoverMemory(ContainerId containerId, long memory) {
    containerVsMemUsage.put(containerId, memory);
    usedMemory += memory;
  }

  /**
   * 恢复容器占用的CPU资源，用于NM重启后恢复已有容器的资源分配状态。
   *
   * @param containerId 需要恢复的容器ID
   * @param cpus 容器占用的CPU核数
   */
  public void recoverCpus(ContainerId containerId, int cpus) {
    containerVsCpusUsage.put(containerId, cpus);
    usedCpus += cpus;
  }

  @Override
  public String toString() {
    return "Node Id:" + nodeId + "\tMemory:" + totalMemory + "\tCPus:"
        + totalCpus;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
    result = prime * result + (int) (totalMemory ^ (totalMemory >>> 32));
    result = prime * result + totalCpus;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    NumaNodeResource other = (NumaNodeResource) obj;
    if (nodeId == null) {
      if (other.nodeId != null) {
        return false;
      }
    } else if (!nodeId.equals(other.nodeId)) {
      return false;
    }
    if (totalMemory != other.totalMemory) {
      return false;
    }
    if (totalCpus != other.totalCpus) {
      return false;
    }
    return true;
  }

  /**
   * 获取当前NUMA节点的ID。
   * @return NUMA节点ID
   */
  public String getNodeId() {
    return nodeId;
  }
}