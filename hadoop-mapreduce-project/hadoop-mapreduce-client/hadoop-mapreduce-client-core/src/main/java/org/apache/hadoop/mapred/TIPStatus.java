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

/**
 * 文件功能：定义MapReduce任务尝试信息（TaskInProgress）的所有状态枚举
 * 描述了TaskInProgress在作业运行过程中的不同运行阶段状态
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum TIPStatus {
  /** 等待调度执行 */
  PENDING, 
  /** 正在运行中 */
  RUNNING, 
  /** 执行完成 */
  COMPLETE, 
  /** 被 killed 终止 */
  KILLED, 
  /** 执行失败 */
  FAILED;
}