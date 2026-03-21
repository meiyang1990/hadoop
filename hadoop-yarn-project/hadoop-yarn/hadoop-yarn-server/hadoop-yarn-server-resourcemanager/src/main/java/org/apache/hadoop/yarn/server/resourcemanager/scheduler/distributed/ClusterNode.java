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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 文件说明：分布式调度中节点负载监控使用的集群节点信息封装类，存储节点资源、队列状态等核心信息
 * 为NodeQueueLoadMonitor的分布式负载均衡调度提供节点状态数据支撑
 */
/**
 * Represents a node in the cluster from the NodeQueueLoadMonitor's perspective
 * 从节点队列负载监控视角对集群节点的抽象封装，存储节点资源和队列状态信息
 */
public class ClusterNode {
  /**
   * 用于初始化/修改ClusterNode属性的参数构造类，统一封装节点更新所需的全部属性
   */
  public static final class Properties {
    private int queueLength = 0;
    private int queueWaitTime = -1;
    private long timestamp;
    private int queueCapacity = 0;
    private boolean queueCapacityIsSet = false;
    private final HashSet<String> labels;
    private Resource capability = null;
    private Resource allocatedResource = null;

    /** 创建空属性实例 */
    public static Properties newInstance() {
      return new Properties();
    }

    /** 设置当前待调度队列长度 */
    Properties setQueueLength(int qLength) {
      this.queueLength = qLength;
      return this;
    }

    /** 设置队列平均等待时间 */
    Properties setQueueWaitTime(int wTime) {
      this.queueWaitTime = wTime;
      return this;
    }

    /** 更新时间戳为当前系统时间 */
    Properties updateTimestamp() {
      this.timestamp = System.currentTimeMillis();
      return this;
    }

    /** 设置队列最大容量 */
    Properties setQueueCapacity(int capacity) {
      this.queueCapacity = capacity;
      this.queueCapacityIsSet = true;
      return this;
    }

    /** 设置节点标签集合 */
    Properties setNodeLabels(Collection<String> labelsToAdd) {
      labels.clear();
      labels.addAll(labelsToAdd);
      return this;
    }

    /** 设置节点总资源能力 */
    Properties setCapability(Resource nodeCapability) {
      this.capability = nodeCapability;
      return this;
    }

    /** 设置节点已分配资源 */
    Properties setAllocatedResource(Resource allocResource) {
      this.allocatedResource = allocResource;
      return this;
    }

    private Properties() {
      labels = new HashSet<>();
    }
  }

  private int queueLength = 0;
  private int queueWaitTime = -1;
  private long timestamp;
  final NodeId nodeId;
  private int queueCapacity = 0;
  private final HashSet<String> labels;
  private Resource capability = Resources.none();
  private Resource allocatedResource = Resources.none();
  private final ReentrantReadWriteLock.WriteLock writeLock;
  private final ReentrantReadWriteLock.ReadLock readLock;

  /**
   * 构造集群节点对象，初始化读写锁和时间戳
   * @param nodeId 节点唯一标识
   */
  public ClusterNode(NodeId nodeId) {
    this.nodeId = nodeId;
    this.labels = new HashSet<>();
    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.writeLock = lock.writeLock();
    this.readLock = lock.readLock();
    this.timestamp = System.currentTimeMillis();
  }

  /**
   * 使用传入的属性批量更新当前节点状态，线程安全
   * @param properties 待更新的属性集合
   * @return 当前节点对象
   */
  public ClusterNode setProperties(final Properties properties) {
    // 获取写锁保证更新原子性
    writeLock.lock();
    try {
      // 更新节点总资源，空属性则设为零资源
      if (properties.capability == null) {
        this.capability = Resources.none();
      } else {
        this.capability = properties.capability;
      }

      // 更新节点已分配资源，空属性则设为零资源
      if (properties.allocatedResource == null) {
        this.allocatedResource = Resources.none();
      } else {
        this.allocatedResource = properties.allocatedResource;
      }

      // 更新队列基础属性
      this.queueLength = properties.queueLength;
      this.queueWaitTime = properties.queueWaitTime;
      this.timestamp = properties.timestamp;
      // 队列容量仅在新增节点时设置，后续节点更新不修改
      if (properties.queueCapacityIsSet) {
        // queue capacity is only set on node add, not on node updates
        this.queueCapacity = properties.queueCapacity;
      }
      // 更新节点标签集合
      this.labels.clear();
      this.labels.addAll(properties.labels);
      return this;
    } finally {
      // 确保写锁释放
      writeLock.unlock();
    }
  }

  /** 获取节点已分配资源，线程安全 */
  public Resource getAllocatedResource() {
    readLock.lock();
    try {
      return this.allocatedResource;
    } finally {
      readLock.unlock();
    }
  }

  /** 计算并获取节点可用资源，线程安全 */
  public Resource getAvailableResource() {
    readLock.lock();
    try {
      return Resources.subtractNonNegative(capability, allocatedResource);
    } finally {
      readLock.unlock();
    }
  }

  /** 获取节点总资源能力，线程安全 */
  public Resource getCapability() {
    readLock.lock();
    try {
      return this.capability;
    } finally {
      readLock.unlock();
    }
  }

  /** 检查节点是否包含指定标签，线程安全 */
  public boolean hasLabel(String label) {
    readLock.lock();
    try {
      return this.labels.contains(label);
    } finally {
      readLock.unlock();
    }
  }

  /** 获取节点状态更新时间戳，线程安全 */
  public long getTimestamp() {
    readLock.lock();
    try {
      return this.timestamp;
    } finally {
      readLock.unlock();
    }
  }

  /** 获取当前队列长度，线程安全 */
  public int getQueueLength() {
    readLock.lock();
    try {
      return this.queueLength;
    } finally {
      readLock.unlock();
    }
  }

  /** 获取队列等待时间，线程安全 */
  public int getQueueWaitTime() {
    readLock.lock();
    try {
      return this.queueWaitTime;
    } finally {
      readLock.unlock();
    }
  }

  /** 获取队列最大容量，线程安全 */
  public int getQueueCapacity() {
    readLock.lock();
    try {
      return this.queueCapacity;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 比较并原子增加已分配资源和队列长度，分配成功则更新状态
   * @param incrementQLen 需要增加的队列长度
   * @param resourceCalculator 资源计算器
   * @param requested 请求分配的资源
   * @return 分配成功返回true，失败返回false
   */
  public boolean compareAndIncrementAllocation(
      final int incrementQLen,
      final ResourceCalculator resourceCalculator,
      final Resource requested) {
    writeLock.lock();
    try {
      // 计算当前可用资源
      final Resource currAvailable = Resources.subtractNonNegative(
          capability, allocatedResource);
      // 检查请求资源是否小于可用资源，足够则直接分配
      if (resourceCalculator.fitsIn(requested, currAvailable)) {
        allocatedResource = Resources.add(allocatedResource, requested);
        return true;
      }

      // 检查请求资源是否超过节点总容量，超过则直接拒绝
      if (!resourceCalculator.fitsIn(requested, capability)) {
        // If does not fit at all, do not allocate
        return false;
      }

      // 资源足够但当前无可用，尝试增加队列长度看是否能容纳
      return compareAndIncrementAllocation(incrementQLen);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 比较并原子增加队列长度，不超过容量则更新
   * @param incrementQLen 需要增加的队列长度
   * @return 增加成功返回true，超过容量返回false
   */
  public boolean compareAndIncrementAllocation(final int incrementQLen) {
    writeLock.lock();
    try {
      final int added = queueLength + incrementQLen;
      if (added <= queueCapacity) {
        queueLength = added;
        return true;
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 检查节点队列是否已满，线程安全
   * @return 队列已满返回true，否则返回false
   */
  public boolean isQueueFull() {
    readLock.lock();
    try {
      return this.queueCapacity > 0 &&
          this.queueLength >= this.queueCapacity;
    } finally {
      readLock.unlock();
    }
  }
}