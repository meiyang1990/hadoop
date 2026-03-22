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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.util.EnumCounters;

import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.CORRUPT;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.DECOMMISSIONED;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.DECOMMISSIONING;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.EXCESS;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.LIVE;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.MAINTENANCE_FOR_READ;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.MAINTENANCE_NOT_FOR_READ;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.READONLY;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.REDUNDANT;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.STALESTORAGE;

/**
 * 文件概述：HDFS数据块副本状态计数器，用于统计一个数据块不同状态副本的数量，不可变对象。
 * 核心职责：存储并提供不同状态（在线、退役中、已退役、损坏、维护中等）副本的数量查询，
 * 为块管理、副本冗余计算、数据块健康检查提供统计依据。
 */
/**
 * A immutable object that stores the number of live replicas and
 * the number of decommissioned Replicas.
 */
public class NumberReplicas extends EnumCounters<NumberReplicas.StoredReplicaState> {

  /**
   * 副本状态枚举，定义了所有需要统计计数的副本状态类型。
   */
  public enum StoredReplicaState {
    // 在线可用副本，条块状文件中排除同一内部块的冗余副本
    LIVE,
    // 只读副本
    READONLY,
    // 正在退役中的副本，条块状文件中排除冗余和在线副本
    DECOMMISSIONING,
    // 已完成退役的副本
    DECOMMISSIONED,
    // 不提供读服务的维护中节点上的副本
    // 计算方式：MAINTENANCE_NOT_FOR_READ = 所有维护中副本 - 可提供读的进入维护节点副本
    MAINTENANCE_NOT_FOR_READ,
    // 可提供读服务的进入维护状态节点的副本
    MAINTENANCE_FOR_READ,
    // 损坏副本
    CORRUPT,
    // 超出复制因子的多余副本，已由块管理器的多余副本表跟踪
    EXCESS,
    // 存储 stale 节点上的副本
    STALESTORAGE,
    // 仅用于条块状文件：尚未被块管理器跟踪的冗余内部块副本数量（不在多余副本表中）
    REDUNDANT
  }

  /**
   * 构造方法，初始化各状态副本计数器。
   */
  public NumberReplicas() {
    super(StoredReplicaState.class);
  }

  /**
   * 获取在线可用副本的数量。
   * @return 在线可用副本数
   */
  public int liveReplicas() {
    return (int) get(LIVE);
  }

  /**
   * 获取只读副本的数量。
   * @return 只读副本数
   */
  public int readOnlyReplicas() {
    return (int) get(READONLY);
  }

  /**
   * 获取正在退役中和已完成退役的副本总数量。
   * @return 退役中+已退役的副本总数
   */
  public int decommissionedAndDecommissioning() {
    return decommissioned() + decommissioning();
  }

  /**
   * 获取已完成退役的副本数量。
   * @return 已退役副本数
   */
  public int decommissioned() {
    return (int) get(DECOMMISSIONED);
  }

  /**
   * 获取正在退役中的副本数量。
   * @return 退役中副本数
   */
  public int decommissioning() {
    return (int) get(DECOMMISSIONING);
  }

  /**
   * 获取损坏副本的数量。
   * @return 损坏副本数
   */
  public int corruptReplicas() {
    return (int) get(CORRUPT);
  }

  /**
   * 获取超出复制因子的多余副本数量。
   * @return 多余副本数
   */
  public int excessReplicas() {
    return (int) get(EXCESS);
  }
  
  /**
   * 获取 stale 节点上的副本数量。
   * 注意：该计数和其他状态计数不互斥，一个副本可以同时被计为在线和 stale。
   * @return stale 节点上的副本数
   */
  public int replicasOnStaleNodes() {
    return (int) get(STALESTORAGE);
  }

  /**
   * 获取条块状文件中未跟踪的冗余内部块副本数量。
   * @return 冗余内部块副本数
   */
  public int redundantInternalBlocks() {
    return (int) get(REDUNDANT);
  }

  /**
   * 获取不提供读服务的维护中副本数量。
   * @return 不提供读的维护中副本数
   */
  public int maintenanceNotForReadReplicas() {
    return (int) get(MAINTENANCE_NOT_FOR_READ);
  }

  /**
   * 获取所有维护中副本的总数量（包含可提供读和不可提供读的）。
   * @return 所有维护中副本总数
   */
  public int maintenanceReplicas() {
    return (int) (get(MAINTENANCE_NOT_FOR_READ) + get(MAINTENANCE_FOR_READ));
  }

  /**
   * 获取所有已停止服务的副本总数（包含维护中和退役类副本）。
   * @return 已停止服务副本总数
   */
  public int outOfServiceReplicas() {
    return maintenanceReplicas() + decommissionedAndDecommissioning();
  }

  /**
   * 获取可提供读服务的正在进入维护状态的副本数量。
   * @return 可提供读的进入维护副本数
   */
  public int liveEnteringMaintenanceReplicas() {
    return (int)get(MAINTENANCE_FOR_READ);
  }
}