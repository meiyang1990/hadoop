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

package org.apache.hadoop.yarn.server.timelineservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * 时间线服务上下文信息封装类，保存流、应用、用户、集群相关标识信息
 * 用于时间线服务中对指标数据进行分层分类标识。
 */
public class TimelineContext {

  private String clusterId;
  private String userId;
  private String flowName;
  private Long flowRunId;
  private String appId;
  private static final Configuration DEFAULT_CONF = new YarnConfiguration();

  /**
   * 空构造函数，所有字段初始化为null/0。
   */
  public TimelineContext() {
    this(null, null, null, 0L, null);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((appId == null) ? 0 : appId.hashCode());
    result = prime * result + ((clusterId == null) ? 0 : clusterId.hashCode());
    result = prime * result + ((flowName == null) ? 0 : flowName.hashCode());
    result = prime * result + ((flowRunId == null) ? 0 : flowRunId.hashCode());
    result = prime * result + ((userId == null) ? 0 : userId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TimelineContext other = (TimelineContext) obj;
    if (appId == null) {
      if (other.appId != null) {
        return false;
      }
    } else if (!appId.equals(other.appId)) {
      return false;
    }
    if (clusterId == null) {
      if (other.clusterId != null) {
        return false;
      }
    } else if (!clusterId.equals(other.clusterId)) {
      return false;
    }
    if (flowName == null) {
      if (other.flowName != null) {
        return false;
      }
    } else if (!flowName.equals(other.flowName)) {
      return false;
    }
    if (flowRunId == null) {
      if (other.flowRunId != null) {
        return false;
      }
    } else if (!flowRunId.equals(other.flowRunId)) {
      return false;
    }
    if (userId == null) {
      if (other.userId != null) {
        return false;
      }
    } else if (!userId.equals(other.userId)) {
      return false;
    }
    return true;
  }

  /**
   * 带参构造函数，创建时间线上下文实例，并自动截断流名称长度。
   * @param clusterId 集群标识
   * @param userId 用户标识
   * @param flowName 工作流名称
   * @param flowRunId 工作流运行实例标识
   * @param appId 应用标识
   */
  public TimelineContext(String clusterId, String userId, String flowName,
      Long flowRunId, String appId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = TimelineUtils.shortenFlowName(flowName, DEFAULT_CONF);
    this.flowRunId = flowRunId;
    this.appId = appId;
  }

  /**
   * 获取集群标识。
   * @return 集群标识
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * 设置集群标识。
   * @param cluster 集群标识
   */
  public void setClusterId(String cluster) {
    this.clusterId = cluster;
  }

  /**
   * 获取用户标识。
   * @return 用户标识
   */
  public String getUserId() {
    return userId;
  }

  /**
   * 设置用户标识。
   * @param user 用户标识
   */
  public void setUserId(String user) {
    this.userId = user;
  }

  /**
   * 获取工作流名称。
   * @return 工作流名称
   */
  public String getFlowName() {
    return flowName;
  }

  /**
   * 设置工作流名称，并自动截断长度。
   * @param flow 工作流名称
   */
  public void setFlowName(String flow) {
    this.flowName = TimelineUtils.shortenFlowName(flow, DEFAULT_CONF);
  }

  /**
   * 获取工作流运行实例标识。
   * @return 工作流运行实例标识
   */
  public Long getFlowRunId() {
    return flowRunId;
  }

  /**
   * 设置工作流运行实例标识。
   * @param runId 工作流运行实例标识
   */
  public void setFlowRunId(long runId) {
    this.flowRunId = runId;
  }

  /**
   * 获取应用标识。
   * @return 应用标识
   */
  public String getAppId() {
    return appId;
  }

  /**
   * 设置应用标识。
   * @param app 应用标识
   */
  public void setAppId(String app) {
    this.appId = app;
  }
}