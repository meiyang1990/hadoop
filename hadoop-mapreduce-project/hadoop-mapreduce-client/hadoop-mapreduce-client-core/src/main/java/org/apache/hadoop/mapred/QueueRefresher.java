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

package org.apache.hadoop.mapred;

import java.util.List;

/**
 * 队列配置刷新抽象基类，为MapReduce任务调度器提供队列配置热更新能力。
 * 任务调度器可以继承该类实现自定义的队列刷新逻辑，QueueManager在接收到管理员
 * 刷新队列配置请求时，会调用该类的refreshQueues方法完成调度器层面的配置更新。
 * 调用方在调用此接口前必须持有对应任务调度器的锁（通常在JobTracker中）。
 */
abstract class QueueRefresher {

  /**
   * 刷新调度器中的队列配置，遵循以下约定：
   * <ol>
   * <li>方法调用前，QueueManager已经完成了对新队列配置的校验，当前不支持
   * 在队列层次结构中新增或删除队列，该限制对所有调度器生效</li>
   * <li>参数传入的是刷新后的根队列列表，所有子队列已经通过根队列正确关联</li>
   * <li>调度器需要从新的根队列信息中提取调度器专属属性，自行完成属性校验并在内部应用更新</li>
   * <li>方法成功返回后，QueueManager会提交本次配置更新，承诺更新后不会出现失败情况；
   * 如果出现异常会导致队列框架不一致，需要重启JobTracker</li>
   * <li>如果调度器在刷新过程中抛出异常，QueueManager会丢弃新配置，保留原有一致的旧配置，
   * 并将错误信息返回给请求发起者</li>
   * </ol>
   * 
   * @param newRootQueues 刷新后的根队列信息列表，包含完整的队列层次结构
   * @throws Throwable 刷新过程中发生任何错误时抛出异常，触发回滚
   */
  abstract void refreshQueues(List<JobQueueInfo> newRootQueues)
      throws Throwable;
}