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
import java.util.Iterator;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.cleanName;

/**
 * YARN公平调度器二级用户组队列放置规则实现类。
 * 当提交应用的用户属于多个用户组时，根据用户的次要用户组，将应用放置到已存在的对应队列中。
 * 会按用户组列表顺序返回第一个匹配到的已配置队列，匹配结果会受到父规则和创建标记的影响。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SecondaryGroupExistingPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecondaryGroupExistingPlacementRule.class);

  // 用户组映射服务提供者，用于获取用户所属的所有用户组
  private Groups groupProvider;

  @Override
  /**
   * 初始化放置规则，获取用户组映射服务
   * @param scheduler 资源调度器实例
   * @return 初始化成功返回true
   * @throws IOException 初始化异常
   */
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    super.initialize(scheduler);
    groupProvider = Groups.
        getUserToGroupsMappingService(((FairScheduler)scheduler).getConfig());

    return true;
  }

  @Override
  /**
   * 为应用计算匹配的放置队列
   * @param asc 应用提交上下文
   * @param user 提交应用的用户名
   * @return 匹配到的放置上下文，未匹配到返回null
   * @throws YarnException 解析用户组失败时抛出异常
   */
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {

    // All users should have at least one group the primary group. If no groups
    // are returned then there is a real issue.
    final Set<String> groupSet;
    // 获取用户所属的所有用户组
    try {
      groupSet = groupProvider.getGroupsSet(user);
    } catch (IOException ioe) {
      throw new YarnException("Group resolution failed", ioe);
    }

    String parentQueue = null;
    PlacementRule parentRule = getParentRule();

    // 如果存在父放置规则，先通过父规则获取父队列
    if (parentRule != null) {
      LOG.debug("SecondaryGroupExisting rule: parent rule found: {}",
          parentRule.getName());
      ApplicationPlacementContext parent =
          parentRule.getPlacementForApp(asc, user);
      // 父规则返回空，或得到的已经是叶子队列，放置失败
      if (parent == null || getQueueManager().
          getQueue(parent.getQueue()) instanceof FSLeafQueue) {
        LOG.debug("SecondaryGroupExisting rule: parent rule failed");
        return null;
      }
      parentQueue = parent.getQueue();
      LOG.debug("SecondaryGroupExisting rule: parent rule result: {}",
          parentQueue);
    }
    // 遍历用户所属所有组，查找匹配的已存在队列
    Iterator<String> it = groupSet.iterator();
    while (it.hasNext()) {
      // 清理组名，移除非法字符
      String group = cleanName(it.next());
      // 拼接完整队列路径
      String queueName =
          parentQueue == null ? assureRoot(group) : parentQueue + DOT + group;
      // 队列已存在且已配置，返回该队列
      if (configuredQueue(queueName)) {
        return new ApplicationPlacementContext(queueName);
      }
    }
    // 未找到匹配的已存在队列，放置失败
    return null;
  }
}