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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.util.Records;

/**
 * 联邦YARN中记录应用归属子集群信息的数据模型，存储应用ID与所在Home子集群的映射关系
 *
 * <p>
 * 包含信息如下：
 * <ul>
 * <li>{@link ApplicationId} - 应用唯一标识</li>
 * <li>{@link SubClusterId} - 应用归属子集群标识</li>
 * </ul>
 *
 */
@Private
@Unstable
public abstract class ApplicationHomeSubCluster {

  /**
   * 创建ApplicationHomeSubCluster实例，仅指定应用ID和归属子集群。
   * @param appId 应用ID
   * @param homeSubCluster 归属子集群ID
   * @return 新建的实例
   */
  @Private
  @Unstable
  public static ApplicationHomeSubCluster newInstance(ApplicationId appId,
      SubClusterId homeSubCluster) {
    ApplicationHomeSubCluster appMapping =
        Records.newRecord(ApplicationHomeSubCluster.class);
    appMapping.setApplicationId(appId);
    appMapping.setHomeSubCluster(homeSubCluster);
    return appMapping;
  }

  /**
   * 创建ApplicationHomeSubCluster实例，指定应用ID、创建时间和归属子集群。
   * @param appId 应用ID
   * @param createTime 应用创建时间
   * @param homeSubCluster 归属子集群ID
   * @return 新建的实例
   */
  @Private
  @Unstable
  public static ApplicationHomeSubCluster newInstance(ApplicationId appId, long createTime,
      SubClusterId homeSubCluster) {
    ApplicationHomeSubCluster appMapping = Records.newRecord(ApplicationHomeSubCluster.class);
    appMapping.setApplicationId(appId);
    appMapping.setHomeSubCluster(homeSubCluster);
    appMapping.setCreateTime(createTime);
    return appMapping;
  }

  /**
   * 创建ApplicationHomeSubCluster实例，指定全量信息。
   * @param appId 应用ID
   * @param createTime 应用创建时间
   * @param homeSubCluster 归属子集群ID
   * @param appSubmissionContext 应用提交上下文
   * @return 新建的实例
   */
  @Private
  @Unstable
  public static ApplicationHomeSubCluster newInstance(ApplicationId appId, long createTime,
      SubClusterId homeSubCluster, ApplicationSubmissionContext appSubmissionContext) {
    ApplicationHomeSubCluster appMapping = Records.newRecord(ApplicationHomeSubCluster.class);
    appMapping.setApplicationId(appId);
    appMapping.setHomeSubCluster(homeSubCluster);
    appMapping.setApplicationSubmissionContext(appSubmissionContext);
    appMapping.setCreateTime(createTime);
    return appMapping;
  }

  /**
   * 创建ApplicationHomeSubCluster实例，指定应用ID、归属子集群和应用提交上下文。
   * @param appId 应用ID
   * @param homeSubCluster 归属子集群ID
   * @param appSubmissionContext 应用提交上下文
   * @return 新建的实例
   */
  @Private
  @Unstable
  public static ApplicationHomeSubCluster newInstance(ApplicationId appId,
      SubClusterId homeSubCluster, ApplicationSubmissionContext appSubmissionContext) {
    ApplicationHomeSubCluster appMapping = Records.newRecord(ApplicationHomeSubCluster.class);
    appMapping.setApplicationId(appId);
    appMapping.setHomeSubCluster(homeSubCluster);
    appMapping.setApplicationSubmissionContext(appSubmissionContext);
    return appMapping;
  }

  /**
   * 获取应用唯一标识。
   *
   * @return 应用ID
   */
  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  /**
   * 设置应用唯一标识。
   *
   * @param applicationId 应用ID
   */
  @Private
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);

  /**
   * 获取应用归属子集群标识，即运行该应用ApplicationMaster的子集群。
   *
   * @return 归属子集群ID
   */
  @Public
  @Unstable
  public abstract SubClusterId getHomeSubCluster();

  /**
   * 设置应用归属子集群标识。
   *
   * @param homeSubCluster 归属子集群ID
   */
  @Private
  @Unstable
  public abstract void setHomeSubCluster(SubClusterId homeSubCluster);

  /**
   * 获取应用创建时间戳。
   *
   * @return 应用创建时间
   */
  @Public
  @Unstable
  public abstract long getCreateTime();

  /**
   * 设置应用创建时间戳。
   *
   * @param time 应用创建时间
   */
  @Private
  @Unstable
  public abstract void setCreateTime(long time);


  /**
   * 设置应用提交上下文，包含应用提交的全部配置信息。
   *
   * @param context 应用提交上下文
   */
  @Private
  @Unstable
  public abstract void setApplicationSubmissionContext(ApplicationSubmissionContext context);

  /**
   * 获取应用提交上下文。
   *
   * @return 应用提交上下文
   */
  @Private
  @Unstable
  public abstract ApplicationSubmissionContext getApplicationSubmissionContext();

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (obj instanceof ApplicationHomeSubCluster) {
      ApplicationHomeSubCluster other = (ApplicationHomeSubCluster) obj;
      return new EqualsBuilder()
          .append(this.getApplicationId(), other.getApplicationId())
          .append(this.getHomeSubCluster(), other.getHomeSubCluster())
          .append(this.getApplicationSubmissionContext(),
          other.getApplicationSubmissionContext())
          .isEquals();
    }

    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(this.getApplicationId()).
        append(this.getHomeSubCluster()).
        append(this.getCreateTime()).
        append(this.getApplicationSubmissionContext())
        .toHashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ApplicationHomeSubCluster: [")
        .append("ApplicationId: ").append(getApplicationId()).append(", ")
        .append("HomeSubCluster: ").append(getHomeSubCluster()).append(", ")
        .append("CreateTime: ").append(getCreateTime()).append(", ")
        .append("ApplicationSubmissionContext: ").append(getApplicationSubmissionContext())
        .append("]");
    return sb.toString();
  }
}