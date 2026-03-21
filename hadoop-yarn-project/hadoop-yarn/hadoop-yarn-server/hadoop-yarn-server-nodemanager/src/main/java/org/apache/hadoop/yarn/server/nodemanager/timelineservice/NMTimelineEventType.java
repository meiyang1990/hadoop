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

package org.apache.hadoop.yarn.server.nodemanager.timelineservice;

/**
 * NodeManager时间线服务事件类型枚举，定义了NM侧时间线服务支持的事件类型。
 * 属于YARN NodeManager时间线服务模块，用于标识不同类型的时间线事件。
 */
public enum NMTimelineEventType {
  /** 发布NodeManager时间线实体事件 */
  TIMELINE_ENTITY_PUBLISH,

  /** 停止并移除时间线客户端事件 */
  STOP_TIMELINE_CLIENT
}