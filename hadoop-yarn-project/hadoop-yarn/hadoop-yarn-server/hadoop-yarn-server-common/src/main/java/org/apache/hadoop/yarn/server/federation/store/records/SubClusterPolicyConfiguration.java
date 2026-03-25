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

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.nio.ByteBuffer;

/**
 * 子集群路由策略配置类，用于在YARN联邦集群中保存单个队列的路由策略配置。
 * 每个配置包含策略类型（对应实现类名）和不透明字节缓冲存储的策略参数，
 * 设计为不透明缓存可以在扩展策略时无需修改联邦状态存储的协议。
 */
@Private
@Unstable
public abstract class SubClusterPolicyConfiguration {


  /**
   * 创建新的子集群策略配置实例，通过参数初始化所有字段。
   * @param queue 目标队列名称
   * @param policyType 策略类型（对应实现类名）
   * @param policyParams 序列化后的策略参数字节缓冲
   * @return 初始化完成的策略配置实例
   */
  @Private
  @Unstable
  public static SubClusterPolicyConfiguration newInstance(String queue,
      String policyType, ByteBuffer policyParams) {
    SubClusterPolicyConfiguration policy =
        Records.newRecord(SubClusterPolicyConfiguration.class);
    policy.setQueue(queue);
    policy.setType(policyType);
    policy.setParams(policyParams);
    return policy;
  }

  /**
   * 基于已有配置创建新的副本实例，深度拷贝所有字段。
   * @param conf 源策略配置实例
   * @return 拷贝完成的新策略配置实例
   */
  @Private
  @Unstable
  public static SubClusterPolicyConfiguration newInstance(
      SubClusterPolicyConfiguration conf) {
    SubClusterPolicyConfiguration policy =
        Records.newRecord(SubClusterPolicyConfiguration.class);
    policy.setQueue(conf.getQueue());
    policy.setType(conf.getType());
    policy.setParams(conf.getParams());
    return policy;
  }

  /**
   * 获取当前策略配置所属队列的名称。
   *
   * @return 队列名称
   */
  @Public
  @Unstable
  public abstract String getQueue();

  /**
   * 设置当前策略配置所属队列的名称。
   *
   * @param queueName 队列名称
   */
  @Private
  @Unstable
  public abstract void setQueue(String queueName);

  /**
   * 获取当前路由策略的类型，常见类型包括随机、轮询、基于负载等，对应具体实现类。
   *
   * @return 策略类型标识（通常为实现类名）
   */
  @Public
  @Unstable
  public abstract String getType();

  /**
   * 设置当前路由策略的类型。
   *
   * @param policyType 策略类型标识
   */
  @Private
  @Unstable
  public abstract void setType(String policyType);

  /**
   * 获取序列化后的策略参数字节缓冲，参数用于控制策略行为，
   * 例如跨子集群队列权重分配配置。
   *
   * @return 包含策略参数的字节缓冲
   */
  @Public
  @Unstable
  public abstract ByteBuffer getParams();

  /**
   * 设置序列化后的策略参数字节缓冲。
   *
   * @param policyParams 包含策略参数的字节缓冲
   */
  @Private
  @Unstable
  public abstract void setParams(ByteBuffer policyParams);

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(this.getType())
        .append(this.getQueue())
        .append(this.getParams()).
        toHashCode();
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (obj instanceof SubClusterPolicyConfiguration) {
      SubClusterPolicyConfiguration other = (SubClusterPolicyConfiguration) obj;
      return new EqualsBuilder()
          .append(this.getType(), other.getType())
          .append(this.getQueue(), other.getQueue())
          .append(this.getParams(), other.getParams())
          .isEquals();
    }

    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SubClusterPolicyConfiguration: [")
        .append("Type: ").append(getType()).append(", ")
        .append("Queue: ").append(getQueue()).append(", ")
        .append("Params: ").append(getParams()).append(", ")
        .append("]");
    return sb.toString();
  }
}