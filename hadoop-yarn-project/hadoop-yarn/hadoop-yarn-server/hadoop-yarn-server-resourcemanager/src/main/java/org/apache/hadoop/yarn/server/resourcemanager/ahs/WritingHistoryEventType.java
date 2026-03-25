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

package org.apache.hadoop.yarn.server.resourcemanager.ahs;

/**
 * 应用历史写入事件类型枚举，定义了需要写入应用历史服务的各类事件类型。
 * 用于标识应用运行生命周期中不同阶段产生的需要持久化的历史事件。
 */
public enum WritingHistoryEventType {
  APP_START, APP_FINISH, APP_ATTEMPT_START, APP_ATTEMPT_FINISH,
  CONTAINER_START, CONTAINER_FINISH
}