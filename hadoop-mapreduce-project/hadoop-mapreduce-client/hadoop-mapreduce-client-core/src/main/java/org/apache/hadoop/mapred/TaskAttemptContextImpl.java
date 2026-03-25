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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Progressable;

/**
 * 旧MapReduce API的任务尝试上下文实现类
 * 适配新MapReduce API的TaskAttemptContextImpl基础实现，兼容旧API接口规范
 * 为旧MapReduce任务执行提供任务尝试级别的上下文环境和进度报告能力
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptContextImpl
       extends org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl 
       implements TaskAttemptContext {
  // 旧API的任务报告器，用于进度、状态和计数器上报
  private Reporter reporter;

  /**
   * 构造任务尝试上下文，使用空报告器
   * @param conf 作业配置对象
   * @param taskid 任务尝试ID
   */
  public TaskAttemptContextImpl(JobConf conf, TaskAttemptID taskid) {
    this(conf, taskid, Reporter.NULL);
  }
  
  /**
   * 构造任务尝试上下文，指定自定义报告器
   * @param conf 作业配置对象
   * @param taskid 任务尝试ID
   * @param reporter 任务报告器实例
   */
  TaskAttemptContextImpl(JobConf conf, TaskAttemptID taskid,
                         Reporter reporter) {
    super(conf, taskid);
    this.reporter = reporter;
  }
  
  /**
   * 获取当前任务尝试ID
   *  
   * @return 旧API格式的任务尝试ID
   */
  public TaskAttemptID getTaskAttemptID() {
    return (TaskAttemptID) super.getTaskAttemptID();
  }
  
  /**
   * 获取进度报告对象
   * @return 包装了Reporter的进度报告实例
   */
  public Progressable getProgressible() {
    return reporter;
  }
  
  /**
   * 获取旧API格式的作业配置
   * @return 作业配置对象JobConf
   */
  public JobConf getJobConf() {
    return (JobConf) getConfiguration();
  }
  
  @Override
  /**
   * 获取当前任务的执行进度
   * @return 进度值，范围0~1
   */
  public float getProgress() {
    return reporter.getProgress();
  }

  @Override
  /**
   * 根据枚举类型获取对应的计数器
   * @param counterName 计数器枚举名称
   * @return 计数器对象
   */
  public Counter getCounter(Enum<?> counterName) {
    return reporter.getCounter(counterName);
  }

  @Override
  /**
   * 根据分组和名称获取对应的计数器
   * @param groupName 计数器分组名称
   * @param counterName 计数器名称
   * @return 计数器对象
   */
  public Counter getCounter(String groupName, String counterName) {
    return reporter.getCounter(groupName, counterName);
  }

  /**
   * 报告任务进度更新
   */
  @Override
  public void progress() {
    reporter.progress();
  }

  /**
   * 设置任务当前状态描述
   * @param status 状态描述字符串
   */
  @Override
  public void setStatus(String status) {
    setStatusString(status);
    reporter.setStatus(status);
  }


}