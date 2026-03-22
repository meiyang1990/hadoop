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
 * MapReduce作业状态枚举，定义了作业生命周期中所有可能的状态
 * 用于标识和跟踪YARN集群中运行的MapReduce作业当前所处阶段
 */
public enum JobState {
  /** 作业刚创建，未初始化 */
  NEW,
  /** 作业已完成初始化，等待调度 */
  INITED,
  /** 作业正在运行中 */
  RUNNING,
  /** 作业执行成功完成 */
  SUCCEEDED,
  /** 作业执行失败 */
  FAILED,
  /** 作业被用户/系统主动杀死 */
  KILLED,
  /** 作业执行遇到错误异常 */
  ERROR
}