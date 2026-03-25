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

import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * YARN资源调度器中队列的抽象接口，定义所有调度队列都需要实现的核心能力
 *
 */
@SuppressWarnings("rawtypes")
@LimitedPrivate("yarn")
public interface SchedulerQueue<T extends SchedulerQueue> extends Queue {

  /**
   * 获取当前队列的所有子队列列表
   * @return 子队列列表
   */
  List<T> getChildQueues();

  /**
   * 获取当前队列的父队列
   * @return 父队列对象，根队列返回null
   */
  T getParent();

  /**
   * 获取当前队列的运行状态
   * @return 队列状态（运行/停止等）
   */
  QueueState getState();

  /**
   * 更新队列的运行状态
   * @param state 待设置的队列状态
   */
  void updateQueueState(QueueState state);

  /**
   * 停止当前队列，不再接受新的应用调度
   */
  void stopQueue();

  /**
   * 激活当前队列，恢复接受新应用的调度
   * @throws YarnException 队列激活失败时抛出异常
   */
  void activateQueue() throws YarnException;
}