// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * 子集群信息实体，存储YARN联邦中参与联邦的子集群运行时信息
 *
 * <p>
 * 包含以下核心信息：
 * <ul>
 * <li>{@link SubClusterId} 子集群唯一标识</li>
 * <li>子集群各类服务地址</li>
 * <li>子集群最近启动时间戳</li>
 * <li>{@link SubClusterState} 子集群当前状态</li>
 * <li>子集群当前容量与利用率信息</li>
 * </ul>
 */
@Private
@Unstable
public abstract class SubClusterInfo {

  /**
   * 创建SubClusterInfo实例，兼容缺少心跳时间的旧版本构造
   * @param subClusterId 子集群唯一标识
   * @param amRMServiceAddress AM-RM服务地址
   * @param clientRMServiceAddress 客户端-RM服务地址
   * @param rmAdminServiceAddress RM管理服务地址
   * @param rmWebServiceAddress RM Web服务地址
   * @param state 子集群状态
   * @param lastStartTime 子集群最近启动时间戳
   * @param capability 子集群容量信息序列化字符串
   * @return 构造完成的SubClusterInfo实例
   */
  @Private
  @Unstable
  @SuppressWarnings("checkstyle:ParameterNumber")
  public static SubClusterInfo newInstance(SubClusterId subClusterId,
      String amRMServiceAddress, String clientRMServiceAddress,
      String rmAdminServiceAddress, String rmWebServiceAddress,
      SubClusterState state, long lastStartTime, String capability) {
    return newInstance(subClusterId, amRMServiceAddress, clientRMServiceAddress,
        rmAdminServiceAddress, rmWebServiceAddress, 0, state, lastStartTime,
        capability);
  }

  /**
   * 创建完整参数的SubClusterInfo实例
   * @param subClusterId 子集群唯一标识
   * @param amRMServiceAddress AM-RM服务地址
   * @param clientRMServiceAddress 客户端-RM服务地址
   * @param rmAdminServiceAddress RM管理服务地址
   * @param rmWebServiceAddress RM Web服务地址
   * @param lastHeartBeat 最近一次心跳时间戳
   * @param state 子集群状态
   * @param lastStartTime 子集群最近启动时间戳
   * @param capability 子集群容量信息序列化字符串
   * @return 构造完成的SubClusterInfo实例
   */
  @Private
  @Unstable
  @SuppressWarnings("checkstyle:ParameterNumber")
  public static SubClusterInfo newInstance(SubClusterId subClusterId,
      String amRMServiceAddress, String clientRMServiceAddress,
      String rmAdminServiceAddress, String rmWebServiceAddress,
      long lastHeartBeat, SubClusterState state, long lastStartTime,
      String capability) {
    // 使用Hadoop Records框架创建实例
    SubClusterInfo subClusterInfo = Records.newRecord(SubClusterInfo.class);
    subClusterInfo.setSubClusterId(subClusterId);
    subClusterInfo.setAMRMServiceAddress(amRMServiceAddress);
    subClusterInfo.setClientRMServiceAddress(clientRMServiceAddress);
    subClusterInfo.setRMAdminServiceAddress(rmAdminServiceAddress);
    subClusterInfo.setRMWebServiceAddress(rmWebServiceAddress);
    subClusterInfo.setLastHeartBeat(lastHeartBeat);
    subClusterInfo.setState(state);
    subClusterInfo.setLastStartTime(lastStartTime);
    subClusterInfo.setCapability(capability);
    return subClusterInfo;
  }

  /**
   * 仅提供核心必要参数创建SubClusterInfo实例
   * @param subClusterId 子集群唯一标识
   * @param rmWebServiceAddress RM Web服务地址
   * @param state 子集群状态
   * @param lastStartTime 子集群最近启动时间戳
   * @param lastHeartBeat 最近一次心跳时间戳
   * @param capability 子集群容量信息序列化字符串
   * @return 构造完成的SubClusterInfo实例
   */
  public static SubClusterInfo newInstance(SubClusterId subClusterId,
      String rmWebServiceAddress, SubClusterState state, long lastStartTime, long lastHeartBeat,
      String capability) {
    return newInstance(subClusterId, null, null, null,
        rmWebServiceAddress, lastHeartBeat, state, lastStartTime, capability);
  }

  /**
   * Get the {@link SubClusterId} representing the unique identifier of the
   * subcluster.
   *
   * @return the subcluster identifier
   */
  @Public
  @Unstable
  public abstract SubClusterId getSubClusterId();

  /**
   * Set the {@link SubClusterId} representing the unique identifier of the
   * subCluster.
   *
   * @param subClusterId the subCluster identifier
   */
  @Private
  @Unstable
  public abstract void setSubClusterId(SubClusterId subClusterId);

  /**
   * Get the URL of the AM-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @return the URL of the AM-RM service endpoint of the subcluster
   *         <code>ResourceManager</code>
   */
  @Public
  @Unstable
  public abstract String getAMRMServiceAddress();

  /**
   * Set the URL of the AM-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @param amRMServiceAddress the URL of the AM-RM service endpoint of the
   *          subcluster <code>ResourceManager</code>
   */
  @Private
  @Unstable
  public abstract void setAMRMServiceAddress(String amRMServiceAddress);

  /**
   * Get the URL of the client-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @return the URL of the client-RM service endpoint of the subcluster
   *         <code>ResourceManager</code>
   */
  @Public
  @Unstable
  public abstract String getClientRMServiceAddress();

  /**
   * Set the URL of the client-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @param clientRMServiceAddress the URL of the client-RM service endpoint of
   *          the subCluster <code>ResourceManager</code>
   */
  @Private
  @Unstable
  public abstract void setClientRMServiceAddress(String clientRMServiceAddress);

  /**
   * Get the URL of the <code>ResourceManager</code> administration service.
   *
   * @return the URL of the <code>ResourceManager</code> administration service
   */
  @Public
  @Unstable
  public abstract String getRMAdminServiceAddress();

  /**
   * Set the URL of the <code>ResourceManager</code> administration service.
   *
   * @param rmAdminServiceAddress the URL of the <code>ResourceManager</code>
   *          administration service.
   */
  @Private
  @Unstable
  public abstract void setRMAdminServiceAddress(String rmAdminServiceAddress);

  /**
   * Get the URL of the <code>ResourceManager</code> web application interface.
   *
   * @return the URL of the <code>ResourceManager</code> web application
   *         interface.
   */
  @Public
  @Unstable
  public abstract String getRMWebServiceAddress();

  /**
   * Set the URL of the <code>ResourceManager</code> web application interface.
   *
   * @param rmWebServiceAddress the URL of the <code>ResourceManager</code> web
   *          application interface.
   */
  @Private
  @Unstable
  public abstract void setRMWebServiceAddress(String rmWebServiceAddress);

  /**
   * Get the last heart beat time of the subcluster.
   *
   * @return the state of the subcluster
   */
  @Public
  @Unstable
  public abstract long getLastHeartBeat();

  /**
   * Set the last heartbeat time of the subcluster.
   *
   * @param time the last heartbeat time of the subcluster
   */
  @Private
  @Unstable
  public abstract void setLastHeartBeat(long time);

  /**
   * Get the {@link SubClusterState} of the subcluster.
   *
   * @return the state of the subcluster
   */
  @Public
  @Unstable
  public abstract SubClusterState getState();

  /**
   * Set the {@link SubClusterState} of the subcluster.
   *
   * @param state the state of the subCluster
   */
  @Private
  @Unstable
  public abstract void setState(SubClusterState state);

  /**
   * Get the timestamp representing the last start time of the subcluster.
   *
   * @return the timestamp representing the last start time of the subcluster
   */
  @Public
  @Unstable
  public abstract long getLastStartTime();

  /**
   * Set the timestamp representing the last start time of the subcluster.
   *
   * @param lastStartTime the timestamp representing the last start time of the
   *          subcluster
   */
  @Private
  @Unstable
  public abstract void setLastStartTime(long lastStartTime);

  /**
   * Get the current capacity and utilization of the subcluster. This is the
   * JAXB marshalled string representation of the <code>ClusterMetrics</code>.
   *
   * @return the current capacity and utilization of the subcluster
   */
  @Public
  @Unstable
  public abstract String getCapability();

  /**
   * Set the current capacity and utilization of the subCluster. This is the
   * JAXB marshalled string representation of the <code>ClusterMetrics</code>.
   *
   * @param capability the current capacity and utilization of the subcluster
   */
  @Private
  @Unstable
  public abstract void setCapability(String capability);

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SubClusterInfo: [")
        .append("SubClusterId: ").append(getSubClusterId()).append(", ")
        .append("AMRMServiceAddress: ").append(getAMRMServiceAddress()).append(", ")
        .append("ClientRMServiceAddress: ").append(getClientRMServiceAddress()).append(", ")
        .append("RMAdminServiceAddress: ").append(getRMAdminServiceAddress()).append(", ")
        .append("RMWebServiceAddress: ").append(getRMWebServiceAddress()).append(", ")
        .append("State: ").append(getState()).append(", ")
        .append("LastStartTime: ").append(getLastStartTime()).append(", ")
        .append("Capability: ").append(getCapability())
        .append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    // 同一对象直接返回相等
    if (this == obj) {
      return true;
    }

    // null对象直接返回不相等
    if (obj == null) {
      return false;
    }

    // 类型不同直接返回不相等
    if (getClass() != obj.getClass()) {
      return false;
    }

    if (obj instanceof SubClusterInfo) {
      SubClusterInfo other = (SubClusterInfo) obj;
      // 仅比较静态标识信息，不包含动态变化的容量和心跳信息
      return new EqualsBuilder()
          .append(this.getSubClusterId(), other.getSubClusterId())
          .append(this.getAMRMServiceAddress(), other.getAMRMServiceAddress())
          .append(this.getClientRMServiceAddress(), other.getClientRMServiceAddress())
          .append(this.getRMAdminServiceAddress(), other.getRMAdminServiceAddress())
          .append(this.getRMWebServiceAddress(), other.getRMWebServiceAddress())
          .append(this.getState(), other.getState())
          .append(this.getLastStartTime(), other.getLastStartTime())
          .isEquals();
    }

    return false;
    // Capability and HeartBeat fields are not included as they are temporal
    // (i.e. timestamps), so they change during the lifetime of the same
    // sub-cluster
  }

  @Override
  public int hashCode() {
    // 仅对静态标识信息计算哈希，不包含动态变化的容量和心跳信息
    return new HashCodeBuilder()
        .append(this.getSubClusterId())
        .append(this.getAMRMServiceAddress())
        .append(this.getClientRMServiceAddress())
        .append(this.getRMAdminServiceAddress())
        .append(this.getRMWebServiceAddress())
        .append(this.getState())
        .append(this.getLastStartTime())
        .toHashCode();
    // Capability and HeartBeat fields are not included as they are temporal
    // (i.e. timestamps), so they change during the lifetime of the same
    // sub-cluster
  }
}