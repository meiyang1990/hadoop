// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.recovery;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTask;

import java.util.List;

/**
 * YARN NodeManager删除任务恢复信息封装类，用于从NM状态存储中恢复DeletionTask。
 * 封装了恢复删除任务所需的全部元数据，支持删除任务依赖关系和调度时间的恢复。
 */
public class DeletionTaskRecoveryInfo {

  // 待恢复的删除任务实例
  private DeletionTask task;
  // 当前任务后续依赖任务ID列表，依赖任务需要在当前任务完成后执行
  private List<Integer> successorTaskIds;
  // 任务计划删除的时间戳
  private long deletionTimestamp;

  /**
   * 构造删除任务恢复信息对象，封装恢复所需全部参数。
   *
   * @param task 待恢复的删除任务实例
   * @param successorTaskIds 当前任务的后续依赖任务ID列表
   * @param deletionTimestamp 任务计划删除的时间戳
   */
  public DeletionTaskRecoveryInfo(DeletionTask task,
      List<Integer> successorTaskIds, long deletionTimestamp) {
    this.task = task;
    this.successorTaskIds = successorTaskIds;
    this.deletionTimestamp = deletionTimestamp;
  }

  /**
   * 获取恢复后的删除任务实例。
   *
   * @return 待恢复的删除任务实例
   */
  public DeletionTask getTask() {
    return task;
  }

  /**
   * 获取当前任务的所有后续依赖任务ID列表。
   *
   * @return 后续依赖任务ID列表
   */
  public List<Integer> getSuccessorTaskIds() {
    return successorTaskIds;
  }

  /**
   * 获取任务计划删除的时间戳。
   *
   * @return 删除时间戳
   */
  public long getDeletionTimestamp() {
    return deletionTimestamp;
  }
}