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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

/**
 * 文件级：MapReduce应用端任务事件基类，封装所有和任务相关的事件公共信息
 * 类级：任务相关事件的基类，承载任务ID和事件类型信息，供不同具体任务事件继承扩展
 */
public class TaskEvent extends AbstractEvent<TaskEventType> {

  // 关联的任务ID
  private TaskId taskID;

  /**
   * 构造任务事件对象，关联指定任务ID和事件类型
   * @param taskID 事件关联的任务ID
   * @param type 任务事件类型
   */
  public TaskEvent(TaskId taskID, TaskEventType type) {
    super(type);
    this.taskID = taskID;
  }

  /**
   * 获取事件关联的任务ID
   * @return 任务ID对象
   */
  public TaskId getTaskID() {
    return taskID;
  }
}