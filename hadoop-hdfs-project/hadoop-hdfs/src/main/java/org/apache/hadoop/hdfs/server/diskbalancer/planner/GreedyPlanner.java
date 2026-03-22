// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerVolumeSet;
import org.apache.hadoop.util.Time;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * 文件说明：磁盘均衡器贪心规划器实现，基于贪心策略生成磁盘数据迁移计划
 * 
 * 贪心规划器是一种简单规划算法，在每一步都计算当前能移动的最大数据块，
 * 通过在超出理想存储和低于理想存储的磁盘之间调度数据移动实现磁盘均衡。
 */
public class GreedyPlanner implements Planner {
  public static final long MB = 1024L * 1024L;
  public static final long GB = MB * 1024L;
  public static final long TB = GB * 1024L;
  private static final Logger LOG =
      LoggerFactory.getLogger(GreedyPlanner.class);
  private final double threshold;

  /**
   * 构造贪心规划器实例
   *
   * @param threshold 磁盘不均衡容忍阈值，当磁盘使用率偏离理想值超过该阈值时需要均衡
   * @param node      当前要进行均衡规划的数据节点
   */
  public GreedyPlanner(double threshold, DiskBalancerDataNode node) {
    this.threshold = threshold;
  }

  /**
   * 为指定数据节点生成磁盘均衡执行计划
   *
   * @param node 待均衡的数据节点
   * @return 生成的均衡节点计划，包含所有数据迁移步骤
   * @throws Exception 规划过程中抛出的异常
   */
  @Override
  public NodePlan plan(DiskBalancerDataNode node) throws Exception {
    // 记录规划开始时间，用于统计耗时
    final long startTime = Time.monotonicNow();
    // 创建空的节点计划，用于保存后续生成的迁移步骤
    NodePlan plan = new NodePlan(node.getDataNodeName(),
        node.getDataNodePort());
    LOG.info("Starting plan for Node : {}:{}",
        node.getDataNodeName(), node.getDataNodePort());
    // 循环规划直到数据节点满足均衡要求
    while (node.isBalancingNeeded(this.threshold)) {
      // 对数据节点上每一组存储类型相同的卷集分别进行均衡规划
      for (DiskBalancerVolumeSet vSet : node.getVolumeSets().values()) {
        balanceVolumeSet(node, vSet, plan);
      }
    }

    // 计算规划耗时并输出日志
    final long endTime = Time.monotonicNow();
    LOG.info("Compute Plan for Node : {}:{} took {} ms",
        node.getDataNodeName(), node.getDataNodePort(), endTime - startTime);

    return plan;
  }

  /**
   * 对指定卷集生成均衡步骤，添加到节点计划中
   *
   * @param node  当前数据节点
   * @param vSet  待均衡的卷集（同一存储类型的一组磁盘）
   * @param plan  节点计划，用于存放生成的迁移步骤
   */
  public void balanceVolumeSet(DiskBalancerDataNode node,
                               DiskBalancerVolumeSet vSet, NodePlan plan)
      throws Exception {
    // 参数非空检查
    Preconditions.checkNotNull(vSet);
    Preconditions.checkNotNull(plan);
    Preconditions.checkNotNull(node);
    // 创建卷集副本，在副本上进行规划计算，不修改原始数据
    DiskBalancerVolumeSet currentSet = new DiskBalancerVolumeSet(vSet);

    // 循环规划直到当前卷集满足均衡要求
    while (currentSet.isBalancingNeeded(this.threshold)) {
      // 移除已经标记为跳过/失败的卷，不参与后续规划
      removeSkipVolumes(currentSet);

      // 按数据密度排序后，取出最空闲和最繁忙的两个磁盘
      DiskBalancerVolume lowVolume = currentSet.getSortedQueue().first();
      DiskBalancerVolume highVolume = currentSet.getSortedQueue().last();

      Step nextStep = null;
      // 两个卷都可参与均衡时，计算本次需要迁移的数据量
      if (!lowVolume.isSkip() && !highVolume.isSkip()) {
        nextStep = computeMove(currentSet, lowVolume, highVolume);
      } else {
        LOG.debug("Skipping compute move. lowVolume: {} highVolume: {}",
            lowVolume.getPath(), highVolume.getPath());
      }

      // 将计算出的迁移步骤应用到当前卷集，更新各卷已使用空间，用于下一步计算
      applyStep(nextStep, currentSet, lowVolume, highVolume);
      // 如果生成了有效迁移步骤，添加到最终节点计划中
      if (nextStep != null) {
        LOG.debug("Step : {} ", nextStep);
        plan.addStep(nextStep);
      }
    }

    LOG.info("Disk Volume set {} - Type : {} plan completed.",
        currentSet.getSetID(),
        currentSet.getVolumes().get(0).getStorageType());

    // 填充节点计划的元信息
    plan.setNodeName(node.getDataNodeName());
    plan.setNodeUUID(node.getDataNodeUUID());
    plan.setTimeStamp(Time.now());
    plan.setPort(node.getDataNodePort());
  }

  /**
   * 将生成的迁移步骤应用到当前卷集，更新卷的使用量并重新计算数据密度，为下一步规划做准备
   *
   * @param nextStep   本次迁移步骤，可为null表示无迁移
   * @param currentSet 当前操作的卷集
   * @param lowVolume  目标卷（接收数据）
   * @param highVolume 源卷（迁出数据）
   */
  private void applyStep(Step nextStep, DiskBalancerVolumeSet currentSet,
                         DiskBalancerVolume lowVolume,
                         DiskBalancerVolume highVolume) throws Exception {

    long used;
    if (nextStep != null) {
      // 增加目标卷已使用空间
      used = lowVolume.getUsed() + nextStep.getBytesToMove();
      lowVolume.setUsed(used);

      // 减少源卷已使用空间
      used = highVolume.getUsed() - nextStep.getBytesToMove();
      highVolume.setUsed(used);
    }

    // 卷数据变更后重新计算所有卷的数据密度
    currentSet.computeVolumeDataDensity();
    printQueue(currentSet.getSortedQueue());
  }

  /**
   * 计算从最繁忙磁盘到最空闲磁盘可迁移的最大数据量
   *
   * @param currentSet 当前操作的卷集
   * @param lowVolume  低数据密度卷（目标卷，接收数据）
   * @param highVolume 高数据密度卷（源卷，迁出数据）
   * @return 生成的迁移步骤，若无法迁移则返回null
   */
  private Step computeMove(DiskBalancerVolumeSet currentSet,
                           DiskBalancerVolume lowVolume,
                           DiskBalancerVolume highVolume) {
    // 计算目标卷最多还能接收多少数据：理想值 - 当前已用
    long maxLowVolumeCanReceive = (long) (
        (currentSet.getIdealUsed() * lowVolume.computeEffectiveCapacity()) -
            lowVolume.getUsed());

    // 如果目标卷已经达到或超过理想值，无法再接收数据，标记为跳过
    if (maxLowVolumeCanReceive <= 0) {
      LOG.debug("{} Skipping disk from computation. Maximum data size " +
          "achieved.", lowVolume.getPath());
      skipVolume(currentSet, lowVolume);
    }

    // 计算源卷最多能迁出多少数据：当前已用 - 理想值
    long maxHighVolumeCanGive = highVolume.getUsed() -
        (long) (currentSet.getIdealUsed() *
            highVolume.computeEffectiveCapacity());
    // 如果源卷已经低于等于理想值，无法再迁出数据，标记为跳过
    if (maxHighVolumeCanGive <= 0) {
      LOG.debug(" {} Skipping disk from computation. Minimum data size " +
          "achieved.", highVolume.getPath());
      skipVolume(currentSet, highVolume);
    }


    // 本次能迁移的数据量为源卷可出 和 目标卷可入 的较小值
    long bytesToMove = Math.min(maxLowVolumeCanReceive, maxHighVolumeCanGive);
    Step nextStep = null;

    // 如果有数据需要迁移，创建迁移步骤实例
    if (bytesToMove > 0) {
      nextStep = new MoveStep(highVolume, currentSet.getIdealUsed(), lowVolume,
          bytesToMove, currentSet.getSetID());
      LOG.debug("Next Step: {}", nextStep);
    }
    return nextStep;
  }

  /**
   * 将指定卷标记为跳过，不再参与后续均衡计算
   *
   * @param currentSet 当前卷集
   * @param volume     需要跳过的卷
   */
  private void skipVolume(DiskBalancerVolumeSet currentSet,
                          DiskBalancerVolume volume) {
    if (LOG.isDebugEnabled()) {
      String message =
          String.format(
              "Skipping volume. Volume : %s " +
              "Type : %s Target " +
              "Number of bytes : %f lowVolume dfsUsed : %d. Skipping this " +
              "volume from all future balancing calls.", volume.getPath(),
              volume.getStorageType(),
              currentSet.getIdealUsed() * volume.getCapacity(),
              volume.getUsed());
      LOG.debug(message);
    }
    volume.setSkip(true);
  }

  /**
   * 从当前卷集中移除所有标记为跳过或失败的卷，更新排序队列
   *
   * @param currentSet 当前操作的卷集
   */
  private void removeSkipVolumes(DiskBalancerVolumeSet currentSet) {
    List<DiskBalancerVolume> volumeList = currentSet.getVolumes();
    Iterator<DiskBalancerVolume> volumeIterator = volumeList.iterator();
    // 遍历迭代器删除已跳过或失败的卷
    while (volumeIterator.hasNext()) {
      DiskBalancerVolume vol = volumeIterator.next();
      if (vol.isSkip() || vol.isFailed()) {
        currentSet.removeVolume(vol);
      }
    }
    // 重新计算数据密度并排序
    currentSet.computeVolumeDataDensity();
    printQueue(currentSet.getSortedQueue());
  }

  /**
   * 调试用函数，打印排序队列中第一个和最后一个卷的数据密度，验证排序是否正确
   *
   * @param queue 排序后的卷队列
   */
  private void printQueue(TreeSet<DiskBalancerVolume> queue) {
    if (LOG.isDebugEnabled()) {
      String format =
          String.format(
              "First Volume : %s, DataDensity : %f, " +
              "Last Volume : %s, DataDensity : %f",
              queue.first().getPath(), queue.first().getVolumeDataDensity(),
              queue.last().getPath(), queue.last().getVolumeDataDensity());
      LOG.debug(format);
    }
  }
}