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
package org.apache.hadoop.mapred.nativetask;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskAttemptID;

/**
 * 本地任务上下文容器，保存MapReduce本地任务运行所需的配置与元信息
 * 用于在本地任务执行过程中传递任务配置、输入输出类型、任务标识等核心上下文信息
 */
@InterfaceAudience.Private
public class TaskContext {
  private final JobConf conf;
  private Class<?> iKClass;
  private Class<?> iVClass;
  private Class<?> oKClass;
  private Class<?> oVClass;
  private final TaskReporter reporter;
  private final TaskAttemptID taskAttemptID;

  /**
   * 构造任务上下文对象，初始化所有核心上下文信息
   * @param conf 作业配置对象
   * @param iKClass 输入键类型
   * @param iVClass 输入值类型
   * @param oKClass 输出键类型
   * @param oVClass 输出值类型
   * @param reporter 任务进度报告器
   * @param id 任务尝试ID
   */
  public TaskContext(JobConf conf, Class<?> iKClass, Class<?> iVClass,
      Class<?> oKClass, Class<?> oVClass, TaskReporter reporter,
      TaskAttemptID id) {
    this.conf = conf;
    this.iKClass = iKClass;
    this.iVClass = iVClass;
    this.oKClass = oKClass;
    this.oVClass = oVClass;
    this.reporter = reporter;
    this.taskAttemptID = id;
  }

  /**
   * 获取输入键的类型
   * @return 输入键Class对象
   */
  public Class<?> getInputKeyClass() {
    return iKClass;
  }

  /**
   * 设置输入键的类型
   * @param klass 输入键Class对象
   */
  public void setInputKeyClass(Class<?> klass) {
    this.iKClass = klass;
  }

  /**
   * 获取输入值的类型
   * @return 输入值Class对象
   */
  public Class<?> getInputValueClass() {
    return iVClass;
  }

  /**
   * 设置输入值的类型
   * @param klass 输入值Class对象
   */
  public void setInputValueClass(Class<?> klass) {
    this.iVClass = klass;
  }

  /**
   * 获取输出键的类型
   * @return 输出键Class对象
   */
  public Class<?> getOutputKeyClass() {
    return this.oKClass;
  }

  /**
   * 设置输出键的类型
   * @param klass 输出键Class对象
   */
  public void setOutputKeyClass(Class<?> klass) {
    this.oKClass = klass;
  }

  /**
   * 获取输出值的类型
   * @return 输出值Class对象
   */
  public Class<?> getOutputValueClass() {
    return this.oVClass;
  }

  /**
   * 设置输出值的类型
   * @param klass 输出值Class对象
   */
  public void setOutputValueClass(Class<?> klass) {
    this.oVClass = klass;
  }

  /**
   * 获取任务进度报告器，用于向框架上报任务进度与状态
   * @return 任务报告器对象
   */
  public TaskReporter getTaskReporter() {
    return this.reporter;
  }

  /**
   * 获取当前任务尝试的唯一标识
   * @return 任务尝试ID对象
   */
  public TaskAttemptID getTaskAttemptId() {
    return this.taskAttemptID;
  }

  /**
   * 获取当前任务的作业配置
   * @return 作业配置对象
   */
  public JobConf getConf() {
    return this.conf;
  }

  /**
   * 创建当前上下文对象的副本
   * @return 新的任务上下文对象，值与原对象一致
   */
  public TaskContext copyOf() {
    return new TaskContext(conf, iKClass, iVClass, oKClass, oVClass, reporter, taskAttemptID);
  }
}