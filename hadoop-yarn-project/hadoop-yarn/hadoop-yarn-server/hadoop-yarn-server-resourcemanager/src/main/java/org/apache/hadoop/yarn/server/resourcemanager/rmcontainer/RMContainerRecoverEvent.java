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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;

/**
 * RM容器恢复事件，ResourceManager重启恢复时，封装从NodeManager获取的容器状态信息。
 */
public class RMContainerRecoverEvent extends RMContainerEvent {

  // NodeManager上报的容器状态信息
  private final NMContainerStatus containerReport;

  /**
   * 构造RM容器恢复事件。
   * @param containerId 容器ID
   * @param containerReport NodeManager上报的容器状态
   */
  public RMContainerRecoverEvent(ContainerId containerId,
      NMContainerStatus containerReport) {
    super(containerId, RMContainerEventType.RECOVER);
    this.containerReport = containerReport;
  }

  /**
   * 获取NodeManager上报的容器状态信息。
   * @return 容器状态
   */
  public NMContainerStatus getContainerReport() {
    return containerReport;
  }
}