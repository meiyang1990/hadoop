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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * 子集群心跳请求类，用于YARN联邦架构中向联邦状态存储上报子集群运行时信息。
 * <p>
 * 包含上报的核心信息如下：
 * <ul>
 * <li>子集群唯一标识 {@link SubClusterId}</li>
 * <li>子集群访问地址</li>
 * <li>子集群上次启动时间戳</li>
 * <li>子集群当前状态 {@link SubClusterState}</li>
 * <li>子集群当前容量和利用率信息</li>
 * </ul>
 */
@Private
@Unstable
public abstract class SubClusterHeartbeatRequest {

  /**
   * 创建新的子集群心跳请求实例，使用默认0作为上次心跳时间。
   * @param subClusterId 子集群唯一标识
   * @param state 子集群当前状态
   * @param capability 子集群容量和利用率信息（JAXB序列化字符串）
   * @return 新的子集群心跳请求实例
   */
  @Private
  @Unstable
  public static SubClusterHeartbeatRequest newInstance(
      SubClusterId subClusterId, SubClusterState state, String capability) {
    return newInstance(subClusterId, 0, state, capability);
  }

  /**
   * 创建新的子集群心跳请求实例，完整参数构造。
   * @param subClusterId 子集群唯一标识
   * @param lastHeartBeat 上次心跳时间戳
   * @param state 子集群当前状态
   * @param capability 子集群容量和利用率信息（JAXB序列化字符串）
   * @return 新的子集群心跳请求实例
   */
  @Private
  @Unstable
  public static SubClusterHeartbeatRequest newInstance(
      SubClusterId subClusterId, long lastHeartBeat, SubClusterState state,
      String capability) {
    // 使用Hadoop Records框架创建实例
    SubClusterHeartbeatRequest subClusterHeartbeatRequest =
        Records.newRecord(SubClusterHeartbeatRequest.class);
    subClusterHeartbeatRequest.setSubClusterId(subClusterId);
    subClusterHeartbeatRequest.setLastHeartBeat(lastHeartBeat);
    subClusterHeartbeatRequest.setState(state);
    subClusterHeartbeatRequest.setCapability(capability);
    return subClusterHeartbeatRequest;
  }

  /**
   * 获取子集群唯一标识符。
   *
   * @return 子集群标识符
   */
  @Public
  @Unstable
  public abstract SubClusterId getSubClusterId();

  /**
   * 设置子集群唯一标识符。
   *
   * @param subClusterId 子集群标识符
   */
  @Private
  @Unstable
  public abstract void setSubClusterId(SubClusterId subClusterId);

  /**
   * 获取子集群上次心跳时间戳。
   *
   * @return 上次心跳时间戳
   */
  @Public
  @Unstable
  public abstract long getLastHeartBeat();

  /**
   * 设置子集群上次心跳时间戳。
   *
   * @param time 上次心跳时间戳
   */
  @Private
  @Unstable
  public abstract void setLastHeartBeat(long time);

  /**
   * 获取子集群当前状态。
   *
   * @return 子集群状态
   */
  @Public
  @Unstable
  public abstract SubClusterState getState();

  /**
   * 设置子集群当前状态。
   *
   * @param state 子集群状态
   */
  @Private
  @Unstable
  public abstract void setState(SubClusterState state);

  /**
   * 获取子集群当前容量和利用率信息，为ClusterMetrics的JAXB序列化字符串。
   *
   * @return 子集群容量和利用率信息
   */
  @Public
  @Unstable
  public abstract String getCapability();

  /**
   * 设置子集群当前容量和利用率信息，为ClusterMetrics的JAXB序列化字符串。
   *
   * @param capability 子集群容量和利用率信息
   */
  @Private
  @Unstable
  public abstract void setCapability(String capability);

  @Override
  public String toString() {
    return "SubClusterHeartbeatRequest [getSubClusterId() = "
        + getSubClusterId() + ", getState() = " + getState()
        + ", getLastHeartBeat = " + getLastHeartBeat() + ", getCapability() = "
        + getCapability() + "]";
  }

}