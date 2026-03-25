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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

/**
 * AM启动器事件，封装ApplicationMaster启动相关事件信息
 * 用于YARN ResourceManager内部AM启动器的事件驱动处理
 */
public class AMLauncherEvent extends AbstractEvent<AMLauncherEventType> {

  // 关联的应用尝试实例
  private final RMAppAttempt appAttempt;

  /**
   * 构造AM启动器事件
   * @param type 事件类型
   * @param appAttempt 关联的应用尝试
   */
  public AMLauncherEvent(AMLauncherEventType type, RMAppAttempt appAttempt) {
    super(type);
    this.appAttempt = appAttempt;
  }

  /**
   * 获取事件关联的应用尝试
   * @return 应用尝试实例
   */
  public RMAppAttempt getAppAttempt() {
    return this.appAttempt;
  }

}