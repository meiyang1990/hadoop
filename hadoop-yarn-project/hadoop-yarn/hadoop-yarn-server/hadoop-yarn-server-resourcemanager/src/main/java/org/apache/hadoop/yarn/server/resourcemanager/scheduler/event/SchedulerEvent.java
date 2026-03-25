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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * YARN资源调度器事件基类，所有调度相关事件都继承此类，
 * 基于事件驱动架构实现调度器内部异步事件处理。
 */
public class SchedulerEvent extends AbstractEvent<SchedulerEventType> {
  /**
   * 构造指定类型的调度事件。
   * @param type 调度事件类型
   */
  public SchedulerEvent(SchedulerEventType type) {
    super(type);
  }
}