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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.cleanName;

/**
 * 基于提交用户主要组的应用队列放置规则，将应用放置到对应用户主要组名称的队列中。
 * 属于FairScheduler调度器的队列放置规则实现。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrimaryGroupPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(PrimaryGroupPlacementRule.class);

  // 用户组信息提供者，用于获取用户所属组列表
  private Groups groupProvider;

  /**
   * 初始化放置规则，初始化组信息服务
   * @param scheduler 资源调度器实例
   * @return 初始化成功返回true
   * @throws IO 异常初始化失败时抛出
   */
  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    super.initialize(scheduler);
    // 从FairScheduler配置中获取用户到组映射服务
    groupProvider = Groups.
        getUserToGroupsMappingService(((FairScheduler)scheduler).getConfig());

    return true;
  }

  /**
   * 根据用户主要组计算应用应该放置的队列位置
   * @param asc 应用提交上下文
   * @param user 提交应用的用户名
   * @return 放置上下文，如果放置成功返回队列位置，无法放置返回null
   * @throws YarnException 组解析失败时抛出异常
   */
  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {

    // All users should have at least one group the primary group. If no groups
    // are returned then there is a real issue.
    final Set<String> groupSet;
    try {
      // 获取用户所属的所有组
      groupSet = groupProvider.getGroupsSet(user);
    } catch (IOException ioe) {
      throw new YarnException("Group resolution failed", ioe);
    }
    // 用户无任何组，抛出异常
    if (groupSet.isEmpty()) {
      LOG.error("Group placement rule failed: No groups returned for user {}",
          user);
      throw new YarnException("No groups returned for user " + user);
    }

    // 清理组名称，移除特殊字符
    String cleanGroup = cleanName(groupSet.iterator().next());
    String queueName;
    // 获取父队列放置规则
    PlacementRule parentRule = getParentRule();

    // 如果存在父规则，先通过父规则获取父队列
    if (getParentRule() != null) {
      LOG.debug("PrimaryGroup rule: parent rule found: {}",
          parentRule.getName());
      // 通过父规则计算父队列位置
      ApplicationPlacementContext parent =
          parentRule.getPlacementForApp(asc, user);
      // 父规则返回null或者父队列已经是叶子队列，无法放置，返回null
      if (parent == null || getQueueManager().
          getQueue(parent.getQueue()) instanceof FSLeafQueue) {
        LOG.debug("PrimaryGroup rule: parent rule failed");
        return null;
      }
      LOG.debug("PrimaryGroup rule: parent rule result: {}",
          parent.getQueue());
      // 拼接父队列+当前组作为完整队列名
      queueName = parent.getQueue() + DOT + cleanGroup;
    } else {
      // 没有父规则，保证队列名添加root前缀
      queueName = assureRoot(cleanGroup);
    }

    // If we can create the queue in the rule or the queue exists return it
    // 如果允许创建队列，或者队列已经配置存在，返回放置位置
    if (createQueue || configuredQueue(queueName)) {
      return new ApplicationPlacementContext(queueName);
    }
    // 队列不存在也不允许创建，放置失败返回null
    return null;
  }
}