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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

/**
 * 容器分配器事件，封装MapReduce ApplicationMaster向RM请求容器时的任务尝试相关事件
 * 用于在ApplicationMaster内部事件驱动架构中传递容器分配请求信息
 */
public class ContainerAllocatorEvent extends 
    AbstractEvent<ContainerAllocator.EventType> {
  
  // 关联的任务尝试ID，标识需要分配容器的任务尝试
  private TaskAttemptId attemptID;

  /**
   * 构造容器分配器事件
   * @param attemptID 需要分配容器的任务尝试ID
   * @param type 事件类型
   */
  public ContainerAllocatorEvent(TaskAttemptId attemptID,
      ContainerAllocator.EventType type) {
    super(type);
    this.attemptID = attemptID;
  }

  /**
   * 获取事件关联的任务尝试ID
   * @return 任务尝试ID
   */
  public TaskAttemptId getAttemptID() {
    return attemptID;
  }
}