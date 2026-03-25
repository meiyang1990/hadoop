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

import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * 块重构任务抽象类，由BlockManager用于表示一个通过复制或纠删码重构块的任务，
 * 重构过程通过从源数据节点传输数据到目标节点完成数据恢复。
 * 是复制重构和纠删码重构两种任务类型的公共抽象基类。
 */
abstract class BlockReconstructionWork {

  /** 日志记录器 */
  public static final Logger LOG =
      LoggerFactory.getLogger(BlockReconstructionWork.class);

  /** 需要重构的块信息 */
  private final BlockInfo block;

  /** 块所属文件路径 */
  private final String srcPath;
  /** 块大小 */
  private final long blockSize;
  /** 块存储策略ID */
  private final byte storagePolicyID;

  /**
   * 重构任务的源数据节点数组：
   * 纠删码重构任务有多个源节点，复制重构任务只有1个源节点放在数组首位
   */
  private final DatanodeDescriptor[] srcNodes;
  /** 已经持有该块的节点，选择新目标节点时需要避开这些节点 */
  private final List<DatanodeDescriptor> containingNodes;
  /** BlockPlacementPolicy#chooseTarget 方法所需参数，存活副本存储信息列表 */
  private  final List<DatanodeStorageInfo> liveReplicaStorages;
  /** 还需要额外补充的副本数量 */
  private final int additionalReplRequired;

  /** 选中的目标存储节点数组 */
  private DatanodeStorageInfo[] targets;
  /** 任务优先级，数值越小优先级越高 */
  private final int priority;
  /** 标记是否当前集群没有足够数量的机架用来放置新副本 */
  private boolean notEnoughRack = false;

  /**
   * 构造块重构任务实例
   * @param block 需要重构的块信息
   * @param bc 块所属的块集合（通常是文件目录）
   * @param srcNodes 重构使用的源数据节点数组
   * @param containingNodes 已持有该块的节点列表，选择新目标时需要避开
   * @param liveReplicaStorages 存活副本存储信息，供块放置策略使用
   * @param additionalReplRequired 需要额外补充的副本数量
   * @param priority 任务优先级
   */
  public BlockReconstructionWork(BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor[] srcNodes,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages,
      int additionalReplRequired,
      int priority) {
    this.block = block;
    this.srcPath = bc.getName();
    this.blockSize = block.getNumBytes();
    this.storagePolicyID = bc.getStoragePolicyID();
    this.srcNodes = srcNodes;
    this.containingNodes = containingNodes;
    this.liveReplicaStorages = liveReplicaStorages;
    this.additionalReplRequired = additionalReplRequired;
    this.priority = priority;
    this.targets = null;
  }

  /**
   * 获取选中的重构目标存储节点数组
   * @return 目标存储节点数组
   */
  DatanodeStorageInfo[] getTargets() {
    return targets;
  }

  /**
   * 重置目标存储节点，清空已选中的目标
   */
  void resetTargets() {
    this.targets = null;
  }

  /**
   * 设置选中的重构目标存储节点数组
   * @param targets 目标存储节点数组
   */
  void setTargets(DatanodeStorageInfo[] targets) {
    this.targets = targets;
  }

  /**
   * 获取不可修改的已持有块节点列表
   * @return 已持有块节点列表
   */
  List<DatanodeDescriptor> getContainingNodes() {
    return Collections.unmodifiableList(containingNodes);
  }

  /**
   * 获取任务优先级
   * @return 优先级数值
   */
  public int getPriority() {
    return priority;
  }

  /**
   * 获取当前重构的块信息
   * @return 块信息对象
   */
  public BlockInfo getBlock() {
    return block;
  }

  /**
   * 获取重构任务的源数据节点数组
   * @return 源数据节点数组
   */
  public DatanodeDescriptor[] getSrcNodes() {
    return srcNodes;
  }

  /**
   * 获取块所属文件路径
   * @return 文件路径字符串
   */
  public String getSrcPath() {
    return srcPath;
  }

  /**
   * 获取块大小
   * @return 块大小字节数
   */
  public long getBlockSize() {
    return blockSize;
  }

  /**
   * 获取存储策略ID
   * @return 存储策略ID
   */
  public byte getStoragePolicyID() {
    return storagePolicyID;
  }

  /**
   * 获取存活副本存储信息列表
   * @return 存活副本存储信息列表
   */
  List<DatanodeStorageInfo> getLiveReplicaStorages() {
    return liveReplicaStorages;
  }

  /**
   * 获取需要额外补充的副本数量
   * @return 需要补充的副本数量
   */
  public int getAdditionalReplRequired() {
    return additionalReplRequired;
  }

  /**
   * 标记当前重构任务需要跨机架放置新副本，但集群没有足够可用机架
   */
  void setNotEnoughRack() {
    notEnoughRack = true;
  }

  /**
   * 判断是否存在可用机架不足的情况
   * @return true表示没有足够机架，false表示足够
   */
  boolean hasNotEnoughRack() {
    return notEnoughRack;
  }

  /**
   * 根据块放置策略为当前重构任务选择目标存储节点
   * @param blockplacement 块放置策略实例
   * @param storagePolicySuite 存储策略集合
   * @param excludedNodes 需要排除的节点集合
   */
  abstract void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes);

  /**
   * 将当前重构任务添加到源数据节点的任务队列中等待执行
   * @param numberReplicas 副本数量统计信息
   * @return 是否成功添加任务
   */
  abstract boolean addTaskToDatanode(NumberReplicas numberReplicas);
}