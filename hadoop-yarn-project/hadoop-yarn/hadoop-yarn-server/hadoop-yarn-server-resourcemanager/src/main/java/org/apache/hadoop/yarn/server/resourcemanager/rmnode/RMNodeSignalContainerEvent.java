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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;

/**
 * YARN ResourceManager 节点事件：通知节点向指定容器发送信号事件
 */
public class RMNodeSignalContainerEvent extends RMNodeEvent {

  // 容器信号请求信息，包含目标容器和信号类型
  private SignalContainerRequest signalRequest;

  /**
   * 构造容器信号事件
   * @param nodeId 目标节点ID
   * @param signalRequest 容器信号请求
   */
  public RMNodeSignalContainerEvent(NodeId nodeId,
      SignalContainerRequest signalRequest) {
    super(nodeId, RMNodeEventType.SIGNAL_CONTAINER);
    this.signalRequest = signalRequest;
  }

  /**
   * 获取容器信号请求
   * @return 容器信号请求对象
   */
  public SignalContainerRequest getSignalRequest() {
    return this.signalRequest;
  }

}