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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * 调度器使用的应用尝试报告，封装了单次应用尝试正在使用的资源信息，对外提供应用尝试的状态与容器查询
 */
@Evolving
@LimitedPrivate("yarn")
public class SchedulerAppReport {
  
  private final Collection<RMContainer> live;
  private final Collection<RMContainer> reserved;
  private final boolean pending;
  
  /**
   * 从调度器应用尝试对象构造应用报告
   * @param app 调度器中的应用尝试对象
   */
  public SchedulerAppReport(SchedulerApplicationAttempt app) {
    this.live = app.getLiveContainers();
    this.reserved = app.getReservedContainers();
    this.pending = app.isPending();
  }
  
  /**
   * 获取所有运行中容器列表
   * @return 所有正在运行的容器集合
   */
  public Collection<RMContainer> getLiveContainers() {
    return live;
  }
  
  /**
   * 获取所有预留容器列表
   * @return 所有已经预留的容器集合
   */
  public Collection<RMContainer> getReservedContainers() {
    return reserved;
  }
  
  /**
   * 查询应用是否处于待调度状态
   * @return true表示应用处于待调度，否则返回false
   */
  public boolean isPending() {
    return pending;
  }
}