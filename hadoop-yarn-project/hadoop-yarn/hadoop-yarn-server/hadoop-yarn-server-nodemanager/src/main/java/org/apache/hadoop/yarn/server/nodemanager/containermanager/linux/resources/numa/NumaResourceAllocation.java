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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 存储容器分配到的NUMA节点资源信息，包含每个NUMA节点分配的内存和CPU数量
 * 用于YARN NodeManager的NUMA感知资源分配功能
 */
public class NumaResourceAllocation implements Serializable {
  private static final long serialVersionUID = 6339719798446595123L;
  // NUMA节点ID到分配内存大小的映射，单位为字节
  private final ImmutableMap<String, Long> nodeVsMemory;
  // NUMA节点ID到分配CPU核心数的映射
  private final ImmutableMap<String, Integer> nodeVsCpus;

  /**
   * 构造NUMA资源分配信息对象
   * @param memoryAllocations 各NUMA节点内存分配结果
   * @param cpuAllocations 各NUMA节点CPU分配结果
   */
  public NumaResourceAllocation(Map<String, Long> memoryAllocations,
      Map<String, Integer> cpuAllocations) {
    nodeVsMemory = ImmutableMap.copyOf(memoryAllocations);
    nodeVsCpus = ImmutableMap.copyOf(cpuAllocations);
  }

  /**
   * 单NUMA节点场景构造NUMA资源分配信息对象
   * @param memNodeId 内存分配所在NUMA节点ID
   * @param memory 分配内存大小
   * @param cpuNodeId CPU分配所在NUMA节点ID
   * @param cpus 分配CPU核心数
   */
  public NumaResourceAllocation(String memNodeId, long memory, String cpuNodeId,
      int cpus) {
    this(ImmutableMap.of(memNodeId, memory), ImmutableMap.of(cpuNodeId, cpus));
  }

  /**
   * 获取分配了内存的所有NUMA节点ID集合
   * @return 分配内存的NUMA节点ID集合
   */
  public Set<String> getMemNodes() {
    return nodeVsMemory.keySet();
  }

  /**
   * 获取分配了CPU的所有NUMA节点ID集合
   * @return 分配CPU的NUMA节点ID集合
   */
  public Set<String> getCpuNodes() {
    return nodeVsCpus.keySet();
  }

  /**
   * 获取所有NUMA节点的内存分配映射
   * @return 不可变的NUMA节点到内存大小映射
   */
  public ImmutableMap<String, Long> getNodeVsMemory() {
    return nodeVsMemory;
  }

  /**
   * 获取所有NUMA节点的CPU分配映射
   * @return 不可变的NUMA节点到CPU核心数映射
   */
  public ImmutableMap<String, Integer> getNodeVsCpus() {
    return nodeVsCpus;
  }

  @Override
  public String toString() {
    return "NumaResourceAllocation{" +
        "nodeVsMemory=" + nodeVsMemory +
        ", nodeVsCpus=" + nodeVsCpus +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NumaResourceAllocation that = (NumaResourceAllocation) o;
    return Objects.equals(nodeVsMemory, that.nodeVsMemory) &&
        Objects.equals(nodeVsCpus, that.nodeVsCpus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeVsMemory, nodeVsCpus);
  }
}