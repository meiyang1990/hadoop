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
 * <p>
 * YARN联邦状态存储中下线子集群的请求封装，用于将子集群状态设置为
 * SC_DECOMMISSIONED（下线中）、SC_LOST（失联）或SC_DEREGISTERED（已注销）。
 *
 * <p>
 * 请求包含以下信息：
 * <ul>
 * <li>{@link SubClusterId} 子集群唯一标识</li>
 * <li>{@link SubClusterState} 目标子集群状态</li>
 * </ul>
 */
@Private
@Unstable
public abstract class SubClusterDeregisterRequest {

  /**
   * 创建新的子集群注销/状态变更请求实例。
   * @param subClusterId 子集群唯一标识
   * @param subClusterState 目标子集群状态
   * @return 初始化完成的请求实例
   */
  @Private
  @Unstable
  public static SubClusterDeregisterRequest newInstance(
      SubClusterId subClusterId, SubClusterState subClusterState) {
    SubClusterDeregisterRequest registerRequest =
        Records.newRecord(SubClusterDeregisterRequest.class);
    registerRequest.setSubClusterId(subClusterId);
    registerRequest.setState(subClusterState);
    return registerRequest;
  }

  /**
   * 获取需要变更状态的子集群唯一标识。
   *
   * @return 子集群唯一标识符
   */
  @Public
  @Unstable
  public abstract SubClusterId getSubClusterId();

  /**
   * 设置需要变更状态的子集群唯一标识。
   *
   * @param subClusterId 子集群唯一标识符
   */
  @Private
  @Unstable
  public abstract void setSubClusterId(SubClusterId subClusterId);

  /**
   * 获取子集群要设置的目标状态。
   *
   * @return 目标子集群状态
   */
  @Public
  @Unstable
  public abstract SubClusterState getState();

  /**
   * 设置子集群要变更的目标状态。
   *
   * @param state 目标子集群状态
   */
  @Private
  @Unstable
  public abstract void setState(SubClusterState state);
}