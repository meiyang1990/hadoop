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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 容量调度器中的叶子队列实现类，位于队列层级最底层，直接容纳应用提交运行。
 * 叶子队列不包含子队列，是实际进行资源分配和应用调度的最小单元。
 */
@Private
@Unstable
public class LeafQueue extends AbstractLeafQueue {
  private static final Logger LOG =
      LoggerFactory.getLogger(LeafQueue.class);

  /**
   * 构造叶子队列实例，默认非动态创建队列。
   * @param queueContext 容量调度器队列上下文，包含全局调度信息
   * @param queueName 队列名称
   * @param parent 父队列
   * @param old 旧队列实例（用于队列配置刷新时重建队列）
   * @throws IOException 初始化失败时抛出异常
   */
  public LeafQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(queueContext, queueName, parent, old, false);
  }

  /**
   * 构造叶子队列实例，支持指定是否为动态创建队列。
   * @param queueContext 容量调度器队列上下文，包含全局调度信息
   * @param queueName 队列名称
   * @param parent 父队列
   * @param old 旧队列实例（用于队列配置刷新时重建队列）
   * @param isDynamic 是否为动态创建队列
   * @throws IOException 初始化失败时抛出异常
   */
  public LeafQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old, boolean isDynamic) throws
      IOException {
    super(queueContext, queueName, parent, old, isDynamic);

    setupQueueConfigs(queueContext.getClusterResource());
  }
}