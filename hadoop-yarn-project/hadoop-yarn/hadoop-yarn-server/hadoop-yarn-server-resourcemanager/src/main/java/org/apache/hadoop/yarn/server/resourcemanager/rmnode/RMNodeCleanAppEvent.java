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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * RM节点清理已完成应用程序的事件。
 * 通知RM节点清理指定应用程序在该节点上的所有残留资源。
 */
public class RMNodeCleanAppEvent extends RMNodeEvent {

  /** 需要清理的应用程序ID */
  private ApplicationId appId;

  /**
   * 构造函数，创建清理指定节点上指定应用的事件。
   * @param nodeId 目标节点ID
   * @param appId 需要清理的应用程序ID
   */
  public RMNodeCleanAppEvent(NodeId nodeId, ApplicationId appId) {
    super(nodeId, RMNodeEventType.CLEANUP_APP);
    this.appId = appId;
  }

  /** 获取需要清理的应用程序ID */
  public ApplicationId getAppId() {
    return this.appId;
  }
}