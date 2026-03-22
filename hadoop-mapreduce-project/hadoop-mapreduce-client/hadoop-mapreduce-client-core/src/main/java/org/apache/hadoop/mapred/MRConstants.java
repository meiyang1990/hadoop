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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * MapReduce框架核心常量定义接口，统一管理MapReduce运行过程中使用的各类常量
 * 包括任务状态码、Shuffle数据传输HTTP头、目录路径、应用标识等公共常量
 */
@Private
@Unstable
public interface MRConstants {
  //
  // 超时与频率常量
  //
  /** 计数器更新上报间隔，单位：毫秒 */
  public static final long COUNTER_UPDATE_INTERVAL = 60 * 1000;

  //
  // 任务执行结果状态码
  //
  /** 任务执行成功状态码 */
  public static int SUCCESS = 0;
  /** 文件未找到错误状态码 */
  public static int FILE_NOT_FOUND = -1;
  
  /**
   * 用于传递Map输出数据长度的自定义HTTP头，Shuffle阶段数据传输使用
   */
  public static final String MAP_OUTPUT_LENGTH = "Map-Output-Length";

  /**
   * 用于传递Map原始输出数据长度的自定义HTTP头，Shuffle阶段数据传输使用
   */
  public static final String RAW_MAP_OUTPUT_LENGTH = "Raw-Map-output-Length";

  /**
   * 用于标识输出数据来源Map任务的HTTP请求头，Shuffle阶段数据传输使用
   */
  public static final String FROM_MAP_TASK = "from-map-task";
  
  /**
   * 用于标识输出数据目标Reduce任务编号的HTTP请求头，Shuffle阶段数据传输使用
   */
  public static final String FOR_REDUCE_TASK = "for-reduce-task";
  
  /** 工作目录名称，主要用于MRv1版本TaskTracker的工作目录定义 */
  public static final String WORKDIR = "work";

  /** 应用尝试ID配置键，用于MRv2版本标识MapReduce作业应用尝试 */
  public static final String APPLICATION_ATTEMPT_ID =
      "mapreduce.job.application.attempt.id";

}