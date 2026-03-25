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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.EnumDoubles;

/**
 * HDFS数据均衡策略抽象基类，定义均衡判定标准和容量利用率计算逻辑。
 * 由于一个DataNode可能包含多个块池，块池级别均衡包含节点级别均衡，反之不成立。
 * 本类提供两种具体均衡策略：按整个DataNode均衡、按DataNode内每个块池分别均衡。
 */
@InterfaceAudience.Private
abstract class BalancingPolicy {
  // 按存储类型分类统计总容量
  final EnumCounters<StorageType> totalCapacities
      = new EnumCounters<StorageType>(StorageType.class);
  // 按存储类型分类统计已用空间
  final EnumCounters<StorageType> totalUsedSpaces
      = new EnumCounters<StorageType>(StorageType.class);
  // 按存储类型分类统计集群平均利用率
  final EnumDoubles<StorageType> avgUtilizations
      = new EnumDoubles<StorageType>(StorageType.class);

  /**
   * 重置所有统计计数器，用于重新计算均衡信息。
   */
  void reset() {
    totalCapacities.reset();
    totalUsedSpaces.reset();
    avgUtilizations.reset();
  }

  /**
   * 获取当前均衡策略的名称。
   * @return 策略名称字符串
   */
  abstract String getName();

  /**
   * 根据数据节点存储报告累计统计全局容量和已用空间。
   * @param r 数据节点存储报告
   */
  abstract void accumulateSpaces(DatanodeStorageReport r);

  /**
   * 根据累计统计结果计算各存储类型的集群平均利用率。
   */
  void initAvgUtilization() {
    for(StorageType t : StorageType.asList()) {
      final long capacity = totalCapacities.get(t);
      if (capacity > 0L) {
        // 利用率计算：已用空间 * 100 / 总容量
        final double avg  = totalUsedSpaces.get(t)*100.0/capacity;
        avgUtilizations.set(t, avg);
      }
    }
  }

  /**
   * 获取指定存储类型的集群平均利用率。
   * @param t 存储类型
   * @return 平均利用率（百分比）
   */
  double getAvgUtilization(StorageType t) {
    return avgUtilizations.get(t);
  }

  /**
   * 计算指定数据节点指定存储类型的利用率。
   * @param r 数据节点存储报告
   * @param t 目标存储类型
   * @return 利用率（百分比），如果节点不包含该存储类型则返回null
   */
  abstract Double getUtilization(DatanodeStorageReport r, StorageType t);
  
  @Override
  public String toString() {
    return BalancingPolicy.class.getSimpleName()
        + "." + getClass().getSimpleName();
  }

  /**
   * 根据策略名称解析得到对应的均衡策略实例。
   * @param s 策略名称字符串
   * @return 匹配的均衡策略实例
   * @throws IllegalArgumentException 无法匹配策略时抛出异常
   */
  static BalancingPolicy parse(String s) {
    final BalancingPolicy [] all = {BalancingPolicy.Node.INSTANCE,
                                    BalancingPolicy.Pool.INSTANCE};
    for(BalancingPolicy p : all) {
      if (p.getName().equalsIgnoreCase(s))
        return p;
    }
    throw new IllegalArgumentException("Cannot parse string \"" + s + "\"");
  }

  /**
   * 节点级别均衡策略：只要每个DataNode整体利用率符合均衡阈值，集群就认为均衡。
   * 按整个DataNode的所有存储聚合计算利用率，不区分块池。
   */
  static class Node extends BalancingPolicy {
    /** 单例实例 */
    static final Node INSTANCE = new Node();
    private Node() {}

    @Override
    String getName() {
      return "datanode";
    }

    @Override
    void accumulateSpaces(DatanodeStorageReport r) {
      for(StorageReport s : r.getStorageReports()) {
        final StorageType t = s.getStorage().getStorageType();
        totalCapacities.add(t, s.getCapacity());
        totalUsedSpaces.add(t, s.getCapacity() - s.getRemaining());
      }
    }
    
    @Override
    Double getUtilization(DatanodeStorageReport r, final StorageType t) {
      long capacity = 0L;
      long totalUsed = 0L;
      for(StorageReport s : r.getStorageReports()) {
        if (s.getStorage().getStorageType() == t) {
          capacity += s.getCapacity();
          totalUsed += s.getCapacity() - s.getRemaining();
        }
      }
      return capacity == 0L ? null : totalUsed * 100.0 / capacity;
    }
  }

  /**
   * 块池级别均衡策略：只有每个DataNode上每个块池的利用率都符合均衡阈值，集群才认为均衡。
   * 联邦场景下每个DataNode会服务多个块池，该策略保证块池层面的数据分布均衡。
   */
  static class Pool extends BalancingPolicy {
    /** 单例实例 */
    static final Pool INSTANCE = new Pool();
    private Pool() {}

    @Override
    String getName() {
      return "blockpool";
    }

    @Override
    void accumulateSpaces(DatanodeStorageReport r) {
      for(StorageReport s : r.getStorageReports()) {
        final StorageType t = s.getStorage().getStorageType();
        // 使用剩余空间 + 块池已用空间作为总容量，而非整个存储容量。
        // 这样可以避免把块往实际剩余空间不足的节点移动，保证利用率计算符合块池实际占用情况：
        // 利用率 = 块池已用空间 / (剩余可用空间 + 块池已用空间)
        // 最终会优先把块迁移到剩余空间多、块池占用少的节点
        totalCapacities.add(t, s.getRemaining() + s.getBlockPoolUsed());
        totalUsedSpaces.add(t, s.getBlockPoolUsed());
      }
    }

    @Override
    Double getUtilization(DatanodeStorageReport r, final StorageType t) {
      long capacity = 0L;
      long blockPoolUsed = 0L;
      for(StorageReport s : r.getStorageReports()) {
        if (s.getStorage().getStorageType() == t) {
          capacity += s.getRemaining() + s.getBlockPoolUsed();
          blockPoolUsed += s.getBlockPoolUsed();
        }
      }
      return capacity == 0L ? null : blockPoolUsed * 100.0 / capacity;
    }
  }
}