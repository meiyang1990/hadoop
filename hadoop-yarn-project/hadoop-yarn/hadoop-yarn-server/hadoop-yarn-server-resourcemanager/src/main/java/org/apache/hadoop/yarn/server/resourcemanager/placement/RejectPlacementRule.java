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
import org.apache.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 文件说明：YARN应用队列放置规则，拒绝所有应用的放置请求，用于拒绝不满足条件的应用提交
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RejectPlacementRule extends FSPlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(RejectPlacementRule.class);

  /**
   * 覆盖配置方法，本规则不使用任何配置，忽略所有传入配置
   * @param initArg 传入的配置参数
   */
  @Override
  public void setConfig(Object initArg) {
    // 本规则忽略所有配置，仅打日志返回
    LOG.debug("RejectPlacementRule instantiated");
  }

  /**
   * 初始化规则，检查是否配置了父规则，Reject规则不允许有父规则
   * @param scheduler 资源调度器实例
   * @return 初始化成功返回true，失败抛出异常
   * @throws IOException 当配置了父规则时抛出IO异常
   */
  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    super.initialize(scheduler);
    if (getParentRule() != null) {
      throw new IOException(
          "Parent rule should not be configured for Reject rule.");
    }
    return true;
  }

  /**
   * 获取应用放置上下文，本规则拒绝所有放置，返回null表示放置失败
   * @param asc 应用提交上下文
   * @param user 提交应用的用户名
   * @return 总是返回null，表示拒绝本次放置
   */
  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) {
    return null;
  }
}