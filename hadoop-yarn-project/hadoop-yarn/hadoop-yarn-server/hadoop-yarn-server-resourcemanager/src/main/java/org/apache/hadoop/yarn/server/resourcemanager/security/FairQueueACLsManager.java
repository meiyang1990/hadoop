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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 公平调度器队列ACL权限检查管理器，实现了{@link QueueACLsManager}接口，
 * 专门为公平调度器提供队列访问权限控制能力。
 */
public class FairQueueACLsManager extends QueueACLsManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(FairQueueACLsManager.class);

  /**
   * 构造公平调度器队列ACL管理器实例。
   * @param scheduler 资源调度器实例
   * @param conf 配置对象
   */
  public FairQueueACLsManager(ResourceScheduler scheduler, Configuration conf) {
    super(scheduler, conf);
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses) {
    // ACL未开启时直接允许访问
    if (!isACLsEnable) {
      return true;
    }
    // 委托调度器检查应用当前所在队列的访问权限
    return scheduler.checkAccess(callerUGI, acl, app.getQueue());
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses,
      String targetQueue) {
    // ACL未开启时直接允许访问
    if (!isACLsEnable) {
      return true;
    }

    // 从公平调度器中获取目标队列实例
    FSQueue queue = ((FairScheduler) scheduler).getQueueManager()
        .getQueue(targetQueue);
    // 目标队列不存在时记录警告日志并拒绝访问
    if (queue == null) {
      LOG.warn("Target queue " + targetQueue
          + " does not exist while trying to move " + app.getApplicationId());
      return false;
    }
    // 委托调度器检查目标队列的访问权限
    return scheduler.checkAccess(callerUGI, acl, targetQueue);
  }

}