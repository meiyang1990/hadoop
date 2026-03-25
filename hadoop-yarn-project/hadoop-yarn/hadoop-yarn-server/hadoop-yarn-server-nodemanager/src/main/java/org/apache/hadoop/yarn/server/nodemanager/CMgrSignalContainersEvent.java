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

package org.apache.hadoop.yarn.server.nodemanager;

import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;

/**
 * 容器管理器信号容器事件，承载向一批容器发送信号的请求
 */
public class CMgrSignalContainersEvent extends ContainerManagerEvent {

  // 需要发送信号的容器请求列表
  private List<SignalContainerRequest> containerToSignal;

  /**
   * 构造信号容器事件
   * @param containerToSignal 需要发送信号的容器请求列表
   */
  public CMgrSignalContainersEvent(List<SignalContainerRequest> containerToSignal) {
    super(ContainerManagerEventType.SIGNAL_CONTAINERS);
    this.containerToSignal = containerToSignal;
  }

  /**
   * 获取所有需要发送信号的容器请求列表
   * @return 需要发送信号的容器请求列表
   */
  public List<SignalContainerRequest> getContainersToSignal() {
    return this.containerToSignal;
  }
}