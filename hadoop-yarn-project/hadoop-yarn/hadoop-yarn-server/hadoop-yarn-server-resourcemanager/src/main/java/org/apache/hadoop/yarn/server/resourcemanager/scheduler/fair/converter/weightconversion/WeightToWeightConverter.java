// 这个文件已经全部加上中文注释
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion;

import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;

/**
 * 公平调度器到容量调度器权重转换器，将公平调度器队列权重直接转换为容量调度器权重格式。
 * 用于 Fair Scheduler 配置转 Capacity Scheduler 配置场景。
 */
public class WeightToWeightConverter
    implements CapacityConverter {
  // 根队列名称常量
  private static final String ROOT_QUEUE = "root";

  /**
   * 递归转换当前队列及其所有子队列的权重到容量调度器配置。
   * @param queue 公平调度器当前处理队列
   * @param csConfig 容量调度器配置对象，用于写入转换结果
   */
  @Override
  public void convertWeightsForChildQueues(FSQueue queue,
      CapacitySchedulerConfiguration csConfig) {
    // 获取当前队列的所有子队列
    List<FSQueue> children = queue.getChildQueues();

    // 如果是父队列或存在子队列，则进行权重转换
    if (queue instanceof FSParentQueue || !children.isEmpty()) {
      // 构造当前队列的路径对象
      QueuePath queuePath = new QueuePath(queue.getName());
      // 根队列单独设置权重
      if (queue.getName().equals(ROOT_QUEUE)) {
        csConfig.setNonLabeledQueueWeight(queuePath, queue.getWeight());
      }

      // 遍历所有子队列，逐个设置非标签队列权重
      children.forEach(fsQueue -> csConfig.setNonLabeledQueueWeight(
          new QueuePath(fsQueue.getName()), fsQueue.getWeight()));
      // 开启当前队列的自动创建队列v2功能
      csConfig.setAutoQueueCreationV2Enabled(queuePath, true);
    }
  }
}