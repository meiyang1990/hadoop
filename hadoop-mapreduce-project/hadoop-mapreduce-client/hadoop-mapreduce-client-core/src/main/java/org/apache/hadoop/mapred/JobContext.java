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
 * 旧版MapReduce API的作业上下文接口
 * 扩展新版org.apache.hadoop.mapreduce.JobContext，提供旧版API兼容能力
 * 用于承载作业运行时的配置信息和进度上报机制
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface JobContext extends org.apache.hadoop.mapreduce.JobContext {
  /**
   * 获取作业的配置对象
   * 
   * @return 作业配置对象JobConf
   */
  public JobConf getJobConf();
  
  /**
   * 获取用于上报作业进度的进度机制对象
   * 
   * @return 进度上报机制对象
   */
  public Progressable getProgressible();
}