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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.utils.Lock;

/**
 * 活跃用户管理器，用于跟踪YARN集群中当前有未完成资源请求的活跃用户
 * 只有当用户存在至少一个运行中且仍有未满足资源请求的应用时，才会被判定为活跃用户
 */
@Private
public class ActiveUsersManager implements AbstractUsersManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ActiveUsersManager.class);
  
  // 队列指标统计对象，用于更新活跃用户相关指标
  private final QueueMetrics metrics;
  
  // 当前活跃用户总数
  private int activeUsers = 0;
  // 存储每个用户对应的活跃应用集合，key为用户名，value为该用户的活跃应用ID列表
  private Map<String, Set<ApplicationId>> usersApplications = 
      new HashMap<String, Set<ApplicationId>>();

  /**
   * 构造活跃用户管理器
   * @param metrics 队列指标统计对象
   */
  public ActiveUsersManager(QueueMetrics metrics) {
    this.metrics = metrics;
  }
  
  /**
   * 激活应用：当应用产生新的未满足资源请求时，标记该应用对应用户为活跃
   * 
   * @param user 应用所属用户名
   * @param applicationId 被激活的应用ID
   */
  @Lock({Queue.class, SchedulerApplicationAttempt.class})
  @Override
  synchronized public void activateApplication(
      String user, ApplicationId applicationId) {
    // 获取该用户当前已有的活跃应用集合
    Set<ApplicationId> userApps = usersApplications.get(user);
    // 用户首次进入活跃状态，初始化集合
    if (userApps == null) {
      userApps = new HashSet<ApplicationId>();
      usersApplications.put(user, userApps);
      // 活跃用户数自增
      ++activeUsers;
      // 更新指标：活跃用户数+1
      metrics.incrActiveUsers();
      LOG.debug("User {} added to activeUsers, currently: {}", user,
          activeUsers);
    }
    // 将应用加入用户活跃集合，新增成功则更新指标
    if (userApps.add(applicationId)) {
      metrics.activateApp(user);
    }
  }
  
  /**
   * 取消激活应用：当应用不再有未满足的资源请求时，取消该应用的活跃标记
   * 如果用户所有应用都不再活跃，则将用户移除出活跃列表
   * 
   * @param user 应用所属用户名
   * @param applicationId 被取消激活的应用ID
   */
  @Lock({Queue.class, SchedulerApplicationAttempt.class})
  @Override
  synchronized public void deactivateApplication(
      String user, ApplicationId applicationId) {
    // 获取该用户当前已有的活跃应用集合
    Set<ApplicationId> userApps = usersApplications.get(user);
    if (userApps != null) {
      // 从活跃集合移除应用，移除成功则更新指标
      if (userApps.remove(applicationId)) {
        metrics.deactivateApp(user);
      }
      // 用户没有更多活跃应用，移除该用户
      if (userApps.isEmpty()) {
        usersApplications.remove(user);
        // 活跃用户数自减
        --activeUsers;
        // 更新指标：活跃用户数-1
        metrics.decrActiveUsers();
        LOG.debug("User {} removed from activeUsers, currently: {}", user,
            activeUsers);
      }
    }
  }

  /**
   * 获取当前活跃用户总数（即存在待处理资源请求的用户数）
   * @return 活跃用户总数
   */
  @Lock({Queue.class, SchedulerApplicationAttempt.class})
  @Override
  synchronized public int getNumActiveUsers() {
    return activeUsers;
  }
}