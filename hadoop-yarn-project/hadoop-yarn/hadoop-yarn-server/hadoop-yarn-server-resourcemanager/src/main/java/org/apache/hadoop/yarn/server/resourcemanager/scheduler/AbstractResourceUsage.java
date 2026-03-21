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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 文件说明：YARN资源调度器抽象资源使用统计基类，用于按节点标签跟踪队列/用户/应用的资源使用情况
 * 核心功能：提供线程安全的资源统计增删改查能力，支持多节点标签分区
 * 
 * 该类可以被用来跟踪队列、用户或应用的资源使用情况
 * 线程安全实现
 */
public class AbstractResourceUsage {
  protected ReadLock readLock;
  protected WriteLock writeLock;
  // 按节点标签存储对应资源使用统计
  protected final Map<String, UsageByLabel> usages;
  // 无标签场景的资源使用统计，单独存储优化访问速度
  private final UsageByLabel noLabelUsages;
  // short for no-label :)

  /**
   * 构造函数，初始化锁和资源统计容器
   */
  public AbstractResourceUsage() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();

    usages = new HashMap<>();

    // For default label, avoid map for faster access.
    noLabelUsages = new UsageByLabel();
    usages.put(CommonNodeLabelsManager.NO_LABEL, noLabelUsages);
  }

  /**
   * 资源统计类型枚举，每种类型对应统计数组中的固定索引位置
   * Use enum here to make implementation more cleaner and readable. Indicates
   * array index for each resource usage type.
   */
  public enum ResourceType {
    // CACHED_USED and CACHED_PENDING may be read by anyone, but must only
    // be written by ordering policies
    USED(0), PENDING(1), AMUSED(2), RESERVED(3), CACHED_USED(4), CACHED_PENDING(
        5), AMLIMIT(6), MIN_RESOURCE(7), MAX_RESOURCE(
            8), EFF_MIN_RESOURCE(9), EFF_MAX_RESOURCE(10), USERAMLIMIT(11);

    private int idx;

    ResourceType(int value) {
      this.idx = value;
    }
  }

  /**
   * 单个节点标签下的资源统计存储类，按资源类型存储对应的资源值
   * UsageByLabel stores resource array for all resource usage types.
   */
  public static class UsageByLabel {
    // 按资源类型索引存储对应资源值，数组索引对应ResourceType的idx
    private final AtomicReferenceArray<Resource> resArr;

    public UsageByLabel() {
      resArr = new AtomicReferenceArray<>(ResourceType.values().length);
      for (int i = 0; i < resArr.length(); i++) {
        resArr.set(i, Resource.newInstance(0, 0));
      }
    }

    /**
     * 获取已使用资源统计
     * @return 已使用资源对象
     */
    public Resource getUsed() {
      return resArr.get(ResourceType.USED.idx);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{used=" + resArr.get(ResourceType.USED.idx) + ", ")
          .append("pending=" + resArr.get(ResourceType.PENDING.idx) + ", ")
          .append("am_used=" + resArr.get(ResourceType.AMUSED.idx) + ", ")
          .append("reserved=" + resArr.get(ResourceType.RESERVED.idx) + ", ")
          .append(
              "min_eff=" + resArr.get(ResourceType.EFF_MIN_RESOURCE.idx) + ", ")
          .append(
              "max_eff=" + resArr.get(ResourceType.EFF_MAX_RESOURCE.idx) + "}");
      return sb.toString();
    }
  }

  /**
   * 空值处理，将null资源转换为零资源对象
   * @param res 输入资源对象
   * @return 非空资源对象，输入为null时返回零资源
   */
  private static Resource normalize(Resource res) {
    if (res == null) {
      return Resources.none();
    }
    return res;
  }

  /**
   * 获取指定节点标签、指定类型的资源使用统计，内部方法
   * @param label 节点标签
   * @param type 资源统计类型
   * @return 对应资源值
   */
  protected Resource _get(String label, ResourceType type) {
    if (label == null || label.equals(RMNodeLabelsManager.NO_LABEL)) {
      return normalize(noLabelUsages.resArr.get(type.idx));
    }

    readLock.lock();
    try {
      UsageByLabel usage = usages.get(label);
      if (null == usage) {
        return Resources.none();
      }
      return normalize(usage.resArr.get(type.idx));
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 汇总所有节点标签下指定类型的资源使用总和，内部方法
   * @param type 资源统计类型
   * @return 所有标签的该类型资源总和
   */
  protected Resource _getAll(ResourceType type) {
    readLock.lock();
    try {
      Resource allOfType = Resources.createResource(0);
      for (Map.Entry<String, UsageByLabel> usageEntry : usages.entrySet()) {
        // all usages types are initialized
        Resources.addTo(allOfType, usageEntry.getValue().resArr.get(type.idx));
      }
      return allOfType;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取指定标签对应的统计对象，如果不存在则创建新对象
   * @param label 节点标签
   * @return 对应统计对象，不存在则新建
   */
  private UsageByLabel getAndAddIfMissing(String label) {
    if (label == null || label.equals(RMNodeLabelsManager.NO_LABEL)) {
      return noLabelUsages;
    }

    if (!usages.containsKey(label)) {
      UsageByLabel u = new UsageByLabel();
      usages.put(label, u);
      return u;
    }

    return usages.get(label);
  }

  /**
   * 设置指定标签、指定类型的资源值，内部方法
   * @param label 节点标签
   * @param type 资源统计类型
   * @param res 要设置的资源值
   */
  protected void _set(String label, ResourceType type, Resource res) {
    writeLock.lock();
    try {
      UsageByLabel usage = getAndAddIfMissing(label);
      usage.resArr.set(type.idx, res);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 增加指定标签、指定类型的资源值，内部方法
   * @param label 节点标签
   * @param type 资源统计类型
   * @param res 要增加的资源量
   */
  protected void _inc(String label, ResourceType type, Resource res) {
    writeLock.lock();
    try {
      UsageByLabel usage = getAndAddIfMissing(label);
      usage.resArr.set(type.idx,
          Resources.add(usage.resArr.get(type.idx), res));
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 减少指定标签、指定类型的资源值，内部方法
   * @param label 节点标签
   * @param type 资源统计类型
   * @param res 要减少的资源量
   */
  protected void _dec(String label, ResourceType type, Resource res) {
    writeLock.lock();
    try {
      UsageByLabel usage = getAndAddIfMissing(label);
      usage.resArr.set(type.idx,
          Resources.subtract(usage.resArr.get(type.idx), res));
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    readLock.lock();
    try {
      return usages.toString();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取当前已存在统计数据的所有节点标签集合
   * @return 节点标签集合
   */
  public Set<String> getExistingNodeLabels() {
    readLock.lock();
    try {
      return new HashSet<>(usages.keySet());
    } finally {
      readLock.unlock();
    }
  }
}