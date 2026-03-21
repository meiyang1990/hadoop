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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN服务端内部远程节点信息封装类，保存节点标识、HTTP访问地址、机架位置、分区等信息，
 * 用于跨节点通信时携带目标节点的完整寻址信息。
 */
@Private
@Unstable
public abstract class RemoteNode implements Comparable<RemoteNode> {

  /**
   * 创建仅包含节点ID和HTTP地址的RemoteNode实例
   * @param nodeId 节点唯一标识
   * @param httpAddress 节点HTTP访问地址
   * @return 新建的RemoteNode实例
   */
  @Private
  @Unstable
  public static RemoteNode newInstance(NodeId nodeId, String httpAddress) {
    RemoteNode remoteNode = Records.newRecord(RemoteNode.class);
    remoteNode.setNodeId(nodeId);
    remoteNode.setHttpAddress(httpAddress);
    return remoteNode;
  }

  /**
   * 创建包含节点ID、HTTP地址和机架名称的RemoteNode实例
   * @param nodeId 节点唯一标识
   * @param httpAddress 节点HTTP访问地址
   * @param rackName 节点所属机架名称
   * @return 新建的RemoteNode实例
   */
  @Private
  @Unstable
  public static RemoteNode newInstance(NodeId nodeId, String httpAddress,
      String rackName) {
    RemoteNode remoteNode = Records.newRecord(RemoteNode.class);
    remoteNode.setNodeId(nodeId);
    remoteNode.setHttpAddress(httpAddress);
    remoteNode.setRackName(rackName);
    return remoteNode;
  }

  /**
   * 创建包含完整节点信息的RemoteNode实例
   * @param nodeId 节点唯一标识
   * @param httpAddress 节点HTTP访问地址
   * @param rackName 节点所属机架名称
   * @param nodePartition 节点分区标识
   * @return 新建的RemoteNode实例
   */
  @Private
  @Unstable
  public static RemoteNode newInstance(NodeId nodeId, String httpAddress,
      String rackName, String nodePartition) {
    RemoteNode remoteNode = Records.newRecord(RemoteNode.class);
    remoteNode.setNodeId(nodeId);
    remoteNode.setHttpAddress(httpAddress);
    remoteNode.setRackName(rackName);
    remoteNode.setNodePartition(nodePartition);
    return remoteNode;
  }

  /**
   * 获取节点唯一标识
   * @return 节点ID
   */
  @Private
  @Unstable
  public abstract NodeId getNodeId();

  /**
   * 设置节点唯一标识
   * @param nodeId 节点ID
   */
  @Private
  @Unstable
  public abstract void setNodeId(NodeId nodeId);

  /**
   * 获取节点HTTP访问地址
   * @return 节点HTTP地址
   */
  @Private
  @Unstable
  public abstract String getHttpAddress();

  /**
   * 设置节点HTTP访问地址
   * @param httpAddress 节点HTTP地址
   */
  @Private
  @Unstable
  public abstract void setHttpAddress(String httpAddress);

  /**
   * 获取节点所属机架名称
   * @return 机架名称
   */
  @Private
  @Unstable
  public abstract String getRackName();

  /**
   * 设置节点所属机架名称
   * @param rackName 机架名称
   */
  @Private
  @Unstable
  public abstract void setRackName(String rackName);

  /**
   * 获取节点分区标识，用于节点分组调度
   * @return 节点分区标识
   */
  @Private
  @Unstable
  public  abstract String getNodePartition();

  /**
   * 设置节点分区标识
   * @param nodePartition 节点分区标识
   */
  @Private
  @Unstable
  public abstract void setNodePartition(String nodePartition);

  /**
   * 基于节点ID比较两个RemoteNode，用于排序集合中有序存储
   * @param other 待比较的另一个RemoteNode
   * @return 比较结果，小于0表示当前节点更小，0表示相等，大于0表示当前节点更大
   */
  @Override
  public int compareTo(RemoteNode other) {
    return this.getNodeId().compareTo(other.getNodeId());
  }

  @Override
  public String toString() {
    return "RemoteNode{" +
        "nodeId=" + getNodeId() + ", " +
        "rackName=" + getRackName() + ", " +
        "httpAddress=" + getHttpAddress() + ", " +
        "partition=" + getNodePartition() + "}";
  }
}