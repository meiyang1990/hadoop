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
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于容量调度器(CapacityScheduler)实现的队列访问权限ACL管理器
 * 实现了{@link QueueACLsManager}接口，为容量调度器提供队列访问权限检查能力
 */
public class CapacityQueueACLsManager extends QueueACLsManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(CapacityQueueACLsManager.class);

  /**
   * 构造容量调度器ACL管理器
   * @param scheduler 资源调度器实例
   * @param conf Hadoop配置对象
   */
  public CapacityQueueACLsManager(ResourceScheduler scheduler,
      Configuration conf) {
    super(scheduler, conf);
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses) {
    // ACL未开启，直接允许访问
    if (!isACLsEnable) {
      return true;
    }

    // 获取应用所在队列
    CSQueue queue = ((CapacityScheduler) scheduler).getQueue(app.getQueue());
    if (queue == null) {
      // 队列名称模糊匹配到多个队列，拒绝访问
      if (((CapacityScheduler) scheduler).isAmbiguous(app.getQueue())) {
        LOG.error("Queue " + app.getQueue() + " is ambiguous for "
            + app.getApplicationId());
        // 无法确定目标队列时拒绝访问
        return false;
      }

      // 应用存在但关联队列不存在，通常是RM重启后该队列已被删除
      // 应用恢复场景下允许用户访问查看已删除队列中的应用
      LOG.error("Queue " + app.getQueue() + " does not exist for "
          + app.getApplicationId());
      return true;
    }
    // 调用授权器检查权限并返回结果
    return authorizer.checkPermission(
        new AccessRequest(queue.getPrivilegedEntity(), callerUGI,
            SchedulerUtils.toAccessType(acl), app.getApplicationId().toString(),
            app.getName(), remoteAddress, forwardedAddresses));

  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses,
      String targetQueue) {
    // ACL未开启，直接允许访问
    if (!isACLsEnable) {
      return true;
    }

    // 该重载方法用于应用移动场景，因容量调度器需要额外目标队列参数，故新增此版本
    // 详情可参考YARN-5554讨论
    CapacityScheduler cs = ((CapacityScheduler) scheduler);
    // 获取移动目标队列
    CSQueue queue = cs.getQueue(targetQueue);
    if (queue == null) {
      // 目标队列不存在或模糊匹配，记录警告日志并拒绝访问
      LOG.warn("Target queue " + targetQueue
          + (cs.isAmbiguous(targetQueue) ? " is ambiguous while trying to move "
              : " does not exist while trying to move ")
          + app.getApplicationId());
      return false;
    }
    // 调用授权器检查权限并返回结果
    return authorizer.checkPermission(
        new AccessRequest(queue.getPrivilegedEntity(), callerUGI,
            SchedulerUtils.toAccessType(acl), app.getApplicationId().toString(),
            app.getName(), remoteAddress, forwardedAddresses));
  }

}