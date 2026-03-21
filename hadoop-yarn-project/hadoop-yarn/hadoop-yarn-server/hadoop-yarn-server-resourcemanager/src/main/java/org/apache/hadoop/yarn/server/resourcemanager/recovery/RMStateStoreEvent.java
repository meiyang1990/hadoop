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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * RM状态存储事件基类，用于RM恢复模块中状态存储相关事件的封装，
 * 配合YARN事件驱动模型处理状态存储操作。
 */
public class RMStateStoreEvent extends AbstractEvent<RMStateStoreEventType> {
  /**
   * 构造指定类型的RM状态存储事件。
   * @param type 事件类型
   */
  public RMStateStoreEvent(RMStateStoreEventType type) {
    super(type);
  }
}