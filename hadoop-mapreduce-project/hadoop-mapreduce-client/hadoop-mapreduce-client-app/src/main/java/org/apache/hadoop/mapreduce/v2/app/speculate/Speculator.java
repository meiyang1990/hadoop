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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.yarn.event.EventHandler;

/**
 * MapReduce任务推测执行组件接口定义。
 * 负责接收任务尝试的状态更新，基于具体推测执行算法判断是否需要启动新的推测尝试，
 * 并发起添加新任务尝试的请求。实现类需要定期扫描作业任务，触发推测执行启动。
 */
public interface Speculator
              extends EventHandler<SpeculatorEvent> {

  /**
   * 推测处理事件类型枚举
   */
  enum EventType {
    /** 任务尝试状态更新事件 */
    ATTEMPT_STATUS_UPDATE,
    /** 任务尝试启动事件 */
    ATTEMPT_START,
    /** 任务容器需求更新事件 */
    TASK_CONTAINER_NEED_UPDATE,
    /** 作业创建事件 */
    JOB_CREATE
  }

  /**
   * 处理任务尝试状态更新。
   * 该方法用于事件在任务尝试状态转换中处理的场景模式
   * @param status 任务尝试当前状态
   */
  public void handleAttempt(TaskAttemptStatus status);
}