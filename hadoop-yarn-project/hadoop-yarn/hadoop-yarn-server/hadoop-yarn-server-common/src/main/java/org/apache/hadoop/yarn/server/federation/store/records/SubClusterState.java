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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * YARN联邦架构中子集群的状态枚举，定义子集群所有可能的运行状态。
 * </p>
 */
@Private
@Unstable
public enum SubClusterState {
  /** 刚完成注册，还未发送第一次心跳的子集群。 */
  SC_NEW,

  /** 注册成功，并且最近按时发送了心跳的健康子集群。 */
  SC_RUNNING,

  /** 子集群处于不健康状态。 */
  SC_UNHEALTHY,

  /** 子集群正在下线过程中。 */
  SC_DECOMMISSIONING,

  /** 子集群已完成下线。 */
  SC_DECOMMISSIONED,

  /** 超过配置时间阈值未收到心跳，判定为丢失的子集群。 */
  SC_LOST,

  /** 子集群已主动注销。 */
  SC_UNREGISTERED;

  /**
   * 检查子集群是否可用于接收新任务调度。
   * @return 是否可用于调度
   */
  public boolean isUsable() {
    return (this == SC_RUNNING || this == SC_NEW);
  }

  /**
   * 检查子集群是否处于活跃运行状态。
   * @return 是否活跃运行
   */
  public boolean isActive() {
    return this == SC_RUNNING;
  }

  /**
   * 检查子集群是否处于不可变更的终态。
   * @return 是否为终态
   */
  public boolean isFinal() {
    return (this == SC_UNREGISTERED || this == SC_DECOMMISSIONED
        || this == SC_LOST);
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(SubClusterState.class);

  /**
   * 将字符串解析为子集群状态枚举。
   *
   * @param state 待解析的状态字符串
   * @return 解析后的子集群状态，解析失败返回null
   */
  public static SubClusterState fromString(String state) {
    try {
      return SubClusterState.valueOf(state);
    } catch (Exception e) {
      LOG.error("Invalid SubCluster State value({}) in the StateStore does not"
          + " match with the YARN Federation standard.", state);
      return null;
    }
  }
}