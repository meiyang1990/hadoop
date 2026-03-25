// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.state.StateTransitionListener;

/**
 * 容器状态转换监听器接口，供外部集群开发者实现，用于在容器状态转换前后接收事件通知
 * 
 * 注意：前置和后置转换回调都会在同步块中串行执行，执行顺序为 preTransition -> 实际状态转换 -> postTransition
 *       实现者必须保证回调方法尽快返回，避免阻塞容器状态机
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ContainerStateTransitionListener extends
    StateTransitionListener<ContainerImpl, ContainerEvent, ContainerState> {

  /**
   * 初始化方法，由NodeManager调用，注入NodeManager上下文对象
   * @param context NodeManager上下文对象
   */
  void init(Context context);
}