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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.assureRoot;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.isValidQueueName;

/**
 * 默认应用队列放置规则，将应用放置到指定的默认队列。
 * 如果未配置默认队列，则将应用放置到root.default队列。
 * 用于公平调度器中，为无法匹配其他放置规则的应用提供默认放置位置。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultPlacementRule.class);

  @VisibleForTesting
  public String defaultQueueName;

  /**
   * 从XML配置节点加载规则配置
   * @param conf 来自{@link FairScheduler#conf}的XML配置元素
   */
  @Override
  public void setConfig(Element conf) {
    // 从配置获取是否允许自动创建队列的标志，未配置时默认为true
    createQueue = getCreateFlag(conf);
    // 存在配置节点时，读取队列名称配置
    if (conf != null) {
      defaultQueueName = conf.getAttribute("queue");
      // 检查配置的队列名称是否合法，不合法则置空，后续使用默认值
      // 这里不做名称清理，因为支持嵌套队列名称
      if (!isValidQueueName(defaultQueueName)) {
        LOG.error("Default rule configured with an illegal queue name: '{}'",
            defaultQueueName);
        defaultQueueName = null;
      }
    }
    // 如果未配置队列名称或配置为空，则使用系统默认的default队列
    if (defaultQueueName == null || defaultQueueName.isEmpty()) {
      defaultQueueName = assureRoot(YarnConfiguration.DEFAULT_QUEUE_NAME);
    } else {
      // 确保队列名称以root前缀开头
      defaultQueueName = assureRoot(defaultQueueName);
    }
    LOG.debug("Default rule instantiated with queue name: {}, " +
        "and create flag: {}", defaultQueueName, createQueue);
  }

  /**
   * 仅通过创建标志配置规则，使用默认队列
   * @param create 是否允许该规则创建队列的标志
   */
  @Override
  public void setConfig(Boolean create) {
    createQueue = create;
    // 未指定队列配置，使用系统默认的default队列
    defaultQueueName = assureRoot(YarnConfiguration.DEFAULT_QUEUE_NAME);
    LOG.debug("Default rule instantiated with default queue name: {}, " +
        "and create flag: {}", defaultQueueName, createQueue);
  }

  /**
   * 初始化放置规则，进行规则合法性检查
   * @param scheduler 资源调度器实例
   * @return 初始化是否成功
   * @throws IOException 初始化失败时抛出异常
   */
  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    super.initialize(scheduler);
    // 默认放置规则不允许配置父规则，必须作为顶级规则
    if (getParentRule() != null) {
      throw new IOException(
          "Parent rule must not be configured for Default rule.");
    }
    return true;
  }

  /**
   * 为应用计算放置的目标队列
   * @param asc 应用提交上下文
   * @param user 提交应用的用户名
   * @return 应用放置上下文，包含目标队列信息；无法放置时返回null
   */
  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) {

    // 如果允许自动创建队列，或者目标队列已存在，则返回目标队列放置
    if (createQueue || configuredQueue(defaultQueueName)) {
      return new ApplicationPlacementContext(defaultQueueName);
    }
    // 不允许创建且队列不存在，返回null让后续规则处理
    return null;
  }
}