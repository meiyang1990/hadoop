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
import org.apache.hadoop.util.Progressable;

/**
 * 旧MapReduce API的任务尝试上下文接口
 * 提供任务尝试执行过程中需要的上下文信息，继承新API的TaskAttemptContext兼容扩展
 * 用于兼容旧版MapReduce接口，为任务尝试提供任务ID、进度报告、作业配置等能力
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface TaskAttemptContext 
       extends org.apache.hadoop.mapreduce.TaskAttemptContext {

  /**
   * 获取当前任务尝试的唯一标识ID
   * @return 当前任务尝试的TaskAttemptID实例
   */
  public TaskAttemptID getTaskAttemptID();

  /**
   * 获取进度报告可回调对象
   * 用于向框架报告任务执行进度
   * @return 进度回调Progressable实例
   */
  public Progressable getProgressible();
  
  /**
   * 获取当前作业的配置对象
   * @return 旧API格式的JobConf作业配置实例
   */
  public JobConf getJobConf();
}