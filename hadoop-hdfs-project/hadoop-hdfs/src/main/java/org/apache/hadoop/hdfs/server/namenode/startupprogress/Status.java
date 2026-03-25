// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * NameNode启动阶段的运行状态枚举，用于描述启动过程中每个阶段的当前执行状态。
 * 在NameNode启动进度追踪中，标识各个启动阶段处于未开始、运行中还是已完成状态。
 */
@InterfaceAudience.Private
public enum Status {
  /**
   * 阶段尚未开始执行，处于等待调度状态。
   */
  PENDING,

  /**
   * 阶段当前正在执行中。
   */
  RUNNING,

  /**
   * 阶段已经执行完成。
   */
  COMPLETE
}