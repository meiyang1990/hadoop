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

package org.apache.hadoop.mapreduce.v2.api.records;

/**
 * 任务尝试Attempt的唯一标识符，MapReduce中每个任务可能会多次尝试执行，每次尝试对应一个TaskAttemptId
 * 
 * 由两部分组成：所属任务的TaskId标识，以及本次尝试的尝试编号，保证全局唯一性
 */
public abstract class TaskAttemptId implements Comparable<TaskAttemptId> {
  /**
   * 获取当前任务尝试所属任务的TaskId
   * @return 所属任务的TaskId
   */
  public abstract TaskId getTaskId();

  /**
   * 获取当前任务尝试的尝试编号
   * @return 尝试编号，从0开始递增
   */
  public abstract int getId();

  /**
   * 设置当前任务尝试所属任务的TaskId
   * @param taskId 所属任务的TaskId
   */
  public abstract void setTaskId(TaskId taskId);

  /**
   * 设置当前任务尝试的尝试编号
   * @param id 尝试编号
   */
  public abstract void setId(int id);

  // 任务尝试标识符前缀，用于字符串格式化输出
  protected static final String TASKATTEMPT = "attempt";

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    // 加入尝试编号计算哈希
    result = prime * result + getId();
    // 加入所属TaskId计算哈希
    result =
        prime * result + ((getTaskId() == null) ? 0 : getTaskId().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    // 同一对象直接返回相等
    if (this == obj)
      return true;
    // 空对象不相等
    if (obj == null)
      return false;
    // 类型不同不相等
    if (getClass() != obj.getClass())
      return false;
    // 强转为TaskAttemptId比较
    TaskAttemptId other = (TaskAttemptId) obj;
    // 尝试编号不同则不相等
    if (getId() != other.getId())
      return false;
    // 所属TaskId不同则不相等
    if (!getTaskId().equals(other.getTaskId()))
      return false;
    return true;
  }

  @Override
  public String toString() {
    // 拼接字符串，添加attempt前缀
    StringBuilder builder = new StringBuilder(TASKATTEMPT);
    TaskId taskId = getTaskId();
    // 追加集群时间戳
    builder.append("_").append(
        taskId.getJobId().getAppId().getClusterTimestamp());
    // 追加格式化后的应用编号
    builder.append("_").append(
        JobId.jobIdFormat.get().format(
            getTaskId().getJobId().getAppId().getId()));
    builder.append("_");
    // 追加任务类型：m代表Map任务，r代表Reduce任务
    builder.append(taskId.getTaskType() == TaskType.MAP ? "m" : "r");
    // 追加格式化后的任务编号
    builder.append("_")
        .append(TaskId.taskIdFormat.get().format(taskId.getId()));
    builder.append("_");
    // 追加尝试编号
    builder.append(getId());
    return builder.toString();
  }

  @Override
  public int compareTo(TaskAttemptId other) {
    // 先比较所属TaskId
    int taskIdComp = this.getTaskId().compareTo(other.getTaskId());
    if (taskIdComp == 0) {
      // TaskId相同再比较尝试编号
      return this.getId() - other.getId();
    } else {
      return taskIdComp;
    }
  }
}