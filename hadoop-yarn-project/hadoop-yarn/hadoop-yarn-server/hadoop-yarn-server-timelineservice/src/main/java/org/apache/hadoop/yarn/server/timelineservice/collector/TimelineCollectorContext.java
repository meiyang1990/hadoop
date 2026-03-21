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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import org.apache.hadoop.yarn.server.timelineservice.TimelineContext;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * 时间线数据收集器写入操作所需上下文信息的封装类
 * 扩展基础TimelineContext，增加流版本信息用于时间线数据维度区分
 */
public class TimelineCollectorContext extends TimelineContext {
  /** 工作流版本 */
  private String flowVersion;

  /**
   * 无参构造函数，初始化空上下文
   */
  public TimelineCollectorContext() {
    this(null, null, null, null, 0L, null);
  }

  /**
   * 全参数构造函数，初始化完整的收集器上下文
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 工作流名称
   * @param flowVersion 工作流版本
   * @param flowRunId 工作流运行ID
   * @param appId 应用ID
   */
  public TimelineCollectorContext(String clusterId, String userId,
      String flowName, String flowVersion, Long flowRunId, String appId) {
    super(clusterId, userId, flowName, flowRunId, appId);
    // 工作流版本为空时使用默认版本
    this.flowVersion = flowVersion == null ?
        TimelineUtils.DEFAULT_FLOW_VERSION : flowVersion;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    // 加入流版本参与哈希计算
    result =
        prime * result + ((flowVersion == null) ? 0 : flowVersion.hashCode());
    return result + super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // 同一对象直接返回相等
    if (this == obj) {
      return true;
    }
    // 父类不相等则整体不相等
    if (!super.equals(obj)) {
      return false;
    }
    TimelineCollectorContext other = (TimelineCollectorContext) obj;
    // 处理流版本为null的情况
    if (flowVersion == null) {
      if (other.flowVersion != null) {
        return false;
      }
    // 流版本值比较
    } else if (!flowVersion.equals(other.flowVersion)) {
      return false;
    }
    return true;
  }

  /**
   * 获取工作流版本
   * @return 工作流版本字符串
   */
  public String getFlowVersion() {
    return flowVersion;
  }

  /**
   * 设置工作流版本
   * @param version 工作流版本字符串
   */
  public void setFlowVersion(String version) {
    this.flowVersion = version;
  }
}