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
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.cleanName;

/**
 * 文件说明：YARN公平调度器基于用户名的应用队列放置规则实现类
 * 核心功能：根据提交应用的用户名，将应用自动放置到对应用户名的队列中
 * 常用于多租户场景，每个用户提交的应用自动进入自己的专属队列
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class UserPlacementRule extends FSPlacementRule {
  // 日志记录器
  private static final Logger LOG =
      LoggerFactory.getLogger(UserPlacementRule.class);

  /**
   * 根据用户名计算应用应该放置的目标队列
   * @param asc 应用提交上下文信息
   * @param user 提交应用的用户名
   * @return 放置结果上下文，返回null表示放置失败
   * @throws YarnException 放置过程中发生异常
   */
  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    String queueName;

    // 清理用户名，移除特殊字符
    String cleanUser = cleanName(user);
    // 获取父队列放置规则
    PlacementRule parentRule = getParentRule();
    if (parentRule != null) {
      LOG.debug("User rule: parent rule found: {}", parentRule.getName());
      // 通过父规则获取父队列
      ApplicationPlacementContext parent =
          parentRule.getPlacementForApp(asc, user);
      // 父规则无结果或父队列已经是叶子队列，无法放置用户队列，放置失败
      if (parent == null || getQueueManager().
          getQueue(parent.getQueue()) instanceof FSLeafQueue) {
        LOG.debug("User rule: parent rule failed");
        return null;
      }
      LOG.debug("User rule: parent rule result: {}", parent.getQueue());
      // 拼接完整队列路径：父队列.用户名
      queueName = parent.getQueue() + DOT + cleanUser;
    } else {
      // 无父规则，用户名作为根下的直接队列
      queueName = assureRoot(cleanUser);
    }

    // 如果允许自动创建队列，或目标队列已存在，则返回目标队列放置结果
    if (createQueue || configuredQueue(queueName)) {
      return new ApplicationPlacementContext(queueName);
    }
    // 不允许创建且队列不存在，放置失败
    return null;
  }
}