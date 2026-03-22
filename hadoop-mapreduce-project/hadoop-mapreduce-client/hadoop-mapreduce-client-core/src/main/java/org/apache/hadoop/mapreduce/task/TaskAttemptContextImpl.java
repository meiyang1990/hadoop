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

package org.apache.hadoop.mapreduce.task;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * MapReduce任务尝试运行上下文实现类，为单个任务尝试实例提供运行时环境和交互接口
 * 封装了任务尝试ID、状态管理、进度上报、计数器统计等核心能力，是任务运行过程中与框架交互的入口
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptContextImpl extends JobContextImpl 
    implements TaskAttemptContext {
  private final TaskAttemptID taskId;
  private String status = "";
  private StatusReporter reporter;

  /**
   * 构造任务尝试上下文，使用空报告器（用于不需要进度/状态上报的场景）
   * @param conf 作业配置对象
   * @param taskId 当前任务尝试的唯一标识
   */
  public TaskAttemptContextImpl(Configuration conf, 
                                TaskAttemptID taskId) {
    this(conf, taskId, new DummyReporter());
  }

  /**
   * 构造任务尝试上下文，使用自定义状态报告器
   * @param conf 作业配置对象
   * @param taskId 当前任务尝试的唯一标识
   * @param reporter 状态/进度/计数器报告器
   */
  public TaskAttemptContextImpl(Configuration conf, 
      TaskAttemptID taskId, StatusReporter reporter) {
    super(conf, taskId.getJobID());
    this.taskId = taskId;
    this.reporter = reporter;
  }

  /**
   * 获取当前任务尝试的唯一标识
   * @return 当前任务尝试的TaskAttemptID对象
   */
  public TaskAttemptID getTaskAttemptID() {
    return taskId;
  }

  /**
   * 获取当前任务设置的状态描述信息
   * @return 当前任务状态字符串
   */
  public String getStatus() {
    return status;
  }

  @Override
  public Counter getCounter(Enum<?> counterName) {
    return reporter.getCounter(counterName);
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return reporter.getCounter(groupName, counterName);
  }

  /**
   * 向框架上报当前任务进度
   */
  @Override
  public void progress() {
    reporter.progress();
  }

  /**
   * 设置上下文内部保存的状态字符串
   * @param status 要设置的状态字符串
   */
  protected void setStatusString(String status) {
    this.status = status;
  }

  /**
   * 设置当前任务的运行状态，同时上报给框架
   * @param status 新的状态描述字符串
   */
  @Override
  public void setStatus(String status) {
    // 对状态字符串进行标准化处理，截断过长的状态文本
    String normalizedStatus = Task.normalizeStatus(status, conf);
    setStatusString(normalizedStatus);
    reporter.setStatus(normalizedStatus);
  }

  /**
   * 空实现状态报告器，用于不需要实际上报进度/状态的场景，仅提供计数器占位
   */
  public static class DummyReporter extends StatusReporter {
    public void setStatus(String s) {
    }
    public void progress() {
    }
    public Counter getCounter(Enum<?> name) {
      return new Counters().findCounter(name);
    }
    public Counter getCounter(String group, String name) {
      return new Counters().findCounter(group, name);
    }
    public float getProgress() {
      return 0f;
    }
  }
  
  @Override
  public float getProgress() {
    return reporter.getProgress();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "TaskAttemptContextImpl{");
    sb.append(super.toString());
    sb.append("; taskId=").append(taskId);
    sb.append(", status='").append(status).append('\'');
    sb.append('}');
    return sb.toString();
  }
}