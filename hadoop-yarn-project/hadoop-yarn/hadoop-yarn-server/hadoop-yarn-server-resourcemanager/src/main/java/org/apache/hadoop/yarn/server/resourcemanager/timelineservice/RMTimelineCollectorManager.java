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

package org.apache.hadoop.yarn.server.resourcemanager.timelineservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollector;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorManager;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * 资源管理器(RM)专属的时间线采集器管理器，扩展通用TimelineCollectorManager提供RM特定实现。
 * 负责为YARN应用初始化时间线采集上下文，处理流信息的提取与设置。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMTimelineCollectorManager extends TimelineCollectorManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMTimelineCollectorManager.class);

  // 持有ResourceManager实例引用，用于获取应用信息
  private ResourceManager rm;

  /**
   * 构造函数，传入ResourceManager实例
   * @param resourceManager ResourceManager实例
   */
  public RMTimelineCollectorManager(ResourceManager resourceManager) {
    super(RMTimelineCollectorManager.class.getName());
    this.rm = resourceManager;
  }

  /**
   * 在放置时间线采集器后的后置处理，初始化应用上下文信息
   * @param appId 应用ID
   * @param collector 时间线采集器实例
   */
  @Override
  protected void doPostPut(ApplicationId appId, TimelineCollector collector) {
    // 从RM上下文获取对应应用实例
    RMApp app = rm.getRMContext().getRMApps().get(appId);
    if (app == null) {
      throw new YarnRuntimeException(
          "Unable to get the timeline collector context info for a " +
          "non-existing app " + appId);
    }
    // 获取应用提交用户
    String userId = app.getUser();
    // 获取采集器上下文对象
    TimelineCollectorContext context = collector.getTimelineEntityContext();
    // 如果用户信息存在，设置到上下文中
    if (userId != null && !userId.isEmpty()) {
      context.setUserId(userId);
    }

    // 为未指定流标签的应用设置默认流信息
    // 默认流名：应用名称(无名称则使用应用ID)，默认流版本："1"，默认流运行ID：应用启动时间
    context.setFlowName(TimelineUtils.generateDefaultFlowName(
        app.getName(), appId));
    context.setFlowVersion(TimelineUtils.DEFAULT_FLOW_VERSION);
    context.setFlowRunId(app.getStartTime());

    // 从应用标签中解析流上下文信息
    for (String tag : app.getApplicationTags()) {
      // 将标签按冒号分割为键值对，最多分割两次保留值中的冒号
      String[] parts = tag.split(":", 2);
      // 格式不合法则跳过
      if (parts.length != 2 || parts[1].isEmpty()) {
        continue;
      }
      // 根据标签键匹配不同的流属性
      switch (parts[0].toUpperCase()) {
      case TimelineUtils.FLOW_NAME_TAG_PREFIX:
        LOG.debug("Setting the flow name: {}", parts[1]);
        // 设置自定义流名
        context.setFlowName(parts[1]);
        break;
      case TimelineUtils.FLOW_VERSION_TAG_PREFIX:
        LOG.debug("Setting the flow version: {}", parts[1]);
        // 设置自定义流版本
        context.setFlowVersion(parts[1]);
        break;
      case TimelineUtils.FLOW_RUN_ID_TAG_PREFIX:
        LOG.debug("Setting the flow run id: {}", parts[1]);
        // 设置自定义流运行ID，转换为长整型
        context.setFlowRunId(Long.parseLong(parts[1]));
        break;
      default:
        break;
      }
    }
  }
}