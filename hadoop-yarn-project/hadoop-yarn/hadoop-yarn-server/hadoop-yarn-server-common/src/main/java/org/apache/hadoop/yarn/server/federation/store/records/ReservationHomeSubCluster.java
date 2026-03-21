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
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * 预约与归属子集群的映射关系实体，存储联邦集群中预约归属哪个子集群的信息。
 *
 * <p>
 * 包含信息如下：
 * <ul>
 * <li>{@link ReservationId} 预约唯一标识</li>
 * <li>{@link SubClusterId} 归属子集群唯一标识</li>
 * </ul>
 *
 */
@Private
@Unstable
public abstract class ReservationHomeSubCluster {

  /**
   * 创建一个新的预约-子集群映射实例。
   * @param resId 预约唯一标识
   * @param homeSubCluster 归属子集群标识
   * @return 初始化完成的映射实例
   */
  @Private
  @Unstable
  public static ReservationHomeSubCluster newInstance(ReservationId resId,
      SubClusterId homeSubCluster) {
    ReservationHomeSubCluster appMapping = Records.newRecord(ReservationHomeSubCluster.class);
    appMapping.setReservationId(resId);
    appMapping.setHomeSubCluster(homeSubCluster);
    return appMapping;
  }

  /**
   * 获取预约的唯一标识。
   *
   * @return 预约标识
   */
  @Public
  @Unstable
  public abstract ReservationId getReservationId();

  /**
   * 设置预约的唯一标识。
   *
   * @param resId 预约标识
   */
  @Private
  @Unstable
  public abstract void setReservationId(ReservationId resId);

  /**
   * 获取预约归属子集群的唯一标识。
   *
   * @return 归属子集群标识
   */
  @Public
  @Unstable
  public abstract SubClusterId getHomeSubCluster();

  /**
   * 设置预约归属子集群的唯一标识。
   *
   * @param subClusterId 归属子集群标识
   */
  @Private
  @Unstable
  public abstract void setHomeSubCluster(SubClusterId subClusterId);

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (obj instanceof ReservationHomeSubCluster) {
      ReservationHomeSubCluster other = (ReservationHomeSubCluster) obj;
      return new EqualsBuilder()
          .append(this.getReservationId(), other.getReservationId())
          .append(this.getHomeSubCluster(), other.getHomeSubCluster())
          .isEquals();
    }

    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(this.getReservationId()).
        append(this.getHomeSubCluster()).
        toHashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReservationHomeSubCluster: [")
        .append("ReservationId: ").append(getReservationId()).append(", ")
        .append("HomeSubCluster: ").append(getHomeSubCluster())
        .append("]");
    return sb.toString();
  }
}