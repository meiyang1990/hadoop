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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

/**
 * 调度活动日志记录级别枚举，定义了不同粒度的活动记录级别
 * 用于控制调度过程中事件记录的详细程度，可按队列、应用、请求、节点不同维度开启记录
 */
public enum ActivityLevel {
  /** 队列级调度活动 */
  QUEUE,
  /** 应用级调度活动 */
  APP,
  /** 资源请求级调度活动 */
  REQUEST,
  /** 节点级调度活动 */
  NODE
}