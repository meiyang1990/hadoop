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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.isValidQueueName;

/**
 * 文件说明：YARN 公平调度器指定队列放置规则实现，将应用放置到用户提交时指定的队列中
 * 
 * Places apps in queues by requested queue of the submitter.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SpecifiedPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpecifiedPlacementRule.class);

  /**
   * 初始化指定队列放置规则，检查配置合法性
   * @param scheduler 资源调度器实例
   * @return 初始化成功返回true
   * @throws IOException 配置错误时抛出异常
   */
  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    super.initialize(scheduler);
    // 指定放置规则不允许配置父规则，若存在则抛出异常
    if (getParentRule() != null) {
      throw new IOException(
          "Parent rule should not be configured for Specified rule.");
    }
    return true;
  }

  /**
   * 根据应用提交信息获取放置目标队列
   * @param asc 应用提交上下文
   * @param user 提交应用的用户名
   * @return 应用放置上下文，返回null表示匹配失败交由下一个规则处理
   * @throws YarnException 队列名称非法时抛出异常
   */
  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {

    // 获取应用提交时指定的队列名称
    String queueName = asc.getQueue();
    // 校验队列名称合法性
    if (!isValidQueueName(queueName)) {
      LOG.error("Specified queue name not valid: '{}'", queueName);
      throw new YarnException("Application submitted by user " + user +
          "with illegal queue name '" + queueName + "'.");
    }
    // 若用户未指定队列，默认会使用default，此时交给下一个规则处理
    if (queueName.equals(YarnConfiguration.DEFAULT_QUEUSE_NAME)) {
      return null;
    }
    // 确保队列名称以根队列开头，补全全路径
    queueName = assureRoot(queueName);
    // 若允许自动创建队列 或 队列已存在，则放置到该队列
    if (createQueue || configuredQueue(queueName)) {
      return new ApplicationPlacementContext(queueName);
    }
    // 目标队列不存在且不允许自动创建，匹配失败交给下一个规则
    return null;
  }
}