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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;

/**
 * NodeManager向ResourceManager发起的注销请求协议记录
 * 用于NodeManager下线时向ResourceManager注销自身节点信息
 */
public abstract class UnRegisterNodeManagerRequest {
  /**
   * 创建NodeManager注销请求实例
   * @param nodeId 待注销节点ID
   * @return 注销请求对象
   */
  public static UnRegisterNodeManagerRequest newInstance(NodeId nodeId) {
    UnRegisterNodeManagerRequest nodeHeartbeatRequest = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    nodeHeartbeatRequest.setNodeId(nodeId);
    return nodeHeartbeatRequest;
  }

  /**
   * 获取待注销节点的ID
   * @return 节点ID
   */
  public abstract NodeId getNodeId();

  /**
   * 设置待注销节点的ID
   * @param nodeId 节点ID
   */
  public abstract void setNodeId(NodeId nodeId);
}