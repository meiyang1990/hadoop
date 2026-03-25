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
 * 子集群注册请求类，用于子集群ResourceManager向联邦状态存储注册加入联邦集群。
 * <p>
 * 请求包含以下信息：
 * <ul>
 * <li>子集群唯一标识 {@link SubClusterId}</li>
 * <li>子集群访问地址</li>
 * <li>子集群最近启动时间戳</li>
 * <li>子集群当前状态 {@code FederationsubClusterState}</li>
 * <li>子集群当前容量和利用率信息</li>
 * </ul>
 */
@Private
@Unstable
public abstract class SubClusterRegisterRequest {

  /**
   * 根据子集群信息构造新的子集群注册请求实例。
   * 
   * @param subClusterInfo 子集群信息对象
   * @return 新的子集群注册请求实例
   */
  @Private
  @Unstable
  public static SubClusterRegisterRequest newInstance(
      SubClusterInfo subClusterInfo) {
    SubClusterRegisterRequest registerSubClusterRequest =
        Records.newRecord(SubClusterRegisterRequest.class);
    registerSubClusterRequest.setSubClusterInfo(subClusterInfo);
    return registerSubClusterRequest;
  }

  /**
   * 获取封装子集群全部信息的SubClusterInfo对象。
   *
   * @return 子集群全部信息
   */
  @Public
  @Unstable
  public abstract SubClusterInfo getSubClusterInfo();

  /**
   * 设置封装子集群全部信息的SubClusterInfo对象。
   *
   * @param subClusterInfo 子集群全部信息
   */
  @Public
  @Unstable
  public abstract void setSubClusterInfo(SubClusterInfo subClusterInfo);

}