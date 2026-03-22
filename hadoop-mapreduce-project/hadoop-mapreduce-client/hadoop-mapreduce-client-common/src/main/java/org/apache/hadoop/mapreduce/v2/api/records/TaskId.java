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

import java.text.NumberFormat;

/**
 * <p>
 * <code>TaskId</code> represents the unique identifier for a Map or Reduce
 * Task.
 * </p>
 * 
 * <p>
 * TaskId consists of 3 parts. First part is <code>JobId</code>, that this Task
 * belongs to. Second part of the TaskId is either 'm' or 'r' representing
 * whether the task is a map task or a reduce task. And the third part is the
 * task number.
 * </p>
 */
/**
 * MapReduce任务唯一标识符抽象类，用于标识Map或Reduce任务
 * 核心职责：维护任务所属作业、任务类型、任务编号信息，提供任务比较、格式化输出能力
 */
public abstract class TaskId implements Comparable<TaskId> {

  /**
   * 获取当前任务所属的作业ID
   * @return 关联的JobId对象
   */
  public abstract JobId getJobId();

  /**
   * 获取当前任务的类型（MAP/REDUCE）
   * @return 任务类型枚举
   */
  public abstract TaskType getTaskType();

  /**
   * 获取当前任务在作业中的编号
   * @return 任务编号
   */
  public abstract int getId();

  /**
   * 设置当前任务所属的作业ID
   * @param jobId 要关联的作业ID
   */
  public abstract void setJobId(JobId jobId);

  /**
   * 设置当前任务的类型
   * @param taskType 任务类型枚举（MAP/REDUCE）
   */
  public abstract void setTaskType(TaskType taskType);

  /**
   * 设置当前任务在作业中的编号
   * @param id 任务编号
   */
  public abstract void setId(int id);

  // TaskId字符串前缀标识
  protected static final String TASK = "task";

  // 线程本地的任务编号格式化器，保证6位数字输出，线程安全
  static final ThreadLocal<NumberFormat> taskIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          // 不使用千分位分组
          fmt.setGroupingUsed(false);
          // 最小保留6位整数长度，不足补零
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getId();
    result = prime * result + getJobId().hashCode();
    result = prime * result + getTaskType().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TaskId other = (TaskId) obj;
    if (getId() != other.getId())
      return false;
    if (!getJobId().equals(other.getJobId()))
      return false;
    if (getTaskType() != other.getTaskType())
      return false;
    return true;
  }
      
  @Override
  public String toString() {
    // 构建标准格式TaskId字符串
    StringBuilder builder = new StringBuilder(TASK);
    JobId jobId = getJobId();
    // 拼接集群时间戳
    builder.append("_").append(jobId.getAppId().getClusterTimestamp());
    // 拼接应用编号
    builder.append("_").append(
        JobId.jobIdFormat.get().format(jobId.getAppId().getId()));
    builder.append("_");
    // 拼接任务类型标识（m表示map，r表示reduce）
    builder.append(getTaskType() == TaskType.MAP ? "m" : "r").append("_");
    // 拼接格式化后的任务编号
    builder.append(taskIdFormat.get().format(getId()));
    return builder.toString();
  }

  @Override
  public int compareTo(TaskId other) {
    // 先比较所属作业ID，作业不同直接返回作业比较结果
    int jobIdComp = this.getJobId().compareTo(other.getJobId());
    if (jobIdComp == 0) {
      // 作业相同，比较任务类型
      if (this.getTaskType() == other.getTaskType()) {
        // 类型相同，比较任务编号
        return this.getId() - other.getId();
      } else {
        return this.getTaskType().compareTo(other.getTaskType());
      }
    } else {
      return jobIdComp;
    }
  }
}