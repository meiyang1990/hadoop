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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import java.util.List;

/**
 * 队列访问控制列表管理器抽象基类，为YARN不同调度器提供队列权限检查的统一接口
 * 负责验证用户对队列提交、管理应用等操作的权限，支持ACL访问控制
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public abstract class QueueACLsManager {

  // 关联的资源调度器
  ResourceScheduler scheduler;
  // ACL访问控制是否启用标识
  boolean isACLsEnable;
  // YARN授权器实例，用于实际权限检查
  YarnAuthorizationProvider authorizer;

  /**
   * 测试用构造方法
   * @param conf 配置对象
   */
  @VisibleForTesting
  public QueueACLsManager(Configuration conf) {
    this(null, new Configuration());
  }

  /**
   * 构造队列ACL管理器
   * @param scheduler 关联的资源调度器
   * @param conf YARN配置对象
   */
  public QueueACLsManager(ResourceScheduler scheduler, Configuration conf) {
    this.scheduler = scheduler;
    // 从配置读取ACL启用状态
    this.isACLsEnable = conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
    // 获取YARN授权器实例
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  /**
   * 根据当前使用的调度器类型，创建对应实现的队列ACL管理器
   * @param scheduler 资源调度器实例
   * @param conf 配置对象
   * @return 对应调度器类型的QueueACLsManager实例
   */
  public static QueueACLsManager getQueueACLsManager(
      ResourceScheduler scheduler, Configuration conf) {
    // 容量调度器使用CapacityQueueACLsManager实现
    if (scheduler instanceof CapacityScheduler) {
      return new CapacityQueueACLsManager(scheduler, conf);
    }
    // 公平调度器使用FairQueueACLsManager实现
    else if (scheduler instanceof FairScheduler) {
      return new FairQueueACLsManager(scheduler, conf);
    }
    // 其他调度器使用通用GenericQueueACLsManager实现
    else {
      return new GenericQueueACLsManager(scheduler, conf);
    }
  }

  /**
   * 检查调用用户对指定应用所属队列是否拥有指定ACL权限
   * @param callerUGI 调用者用户信息
   * @param acl 需要检查的队列ACL权限类型
   * @param app 待检查的应用
   * @param remoteAddress 远程客户端地址
   * @param forwardedAddresses 转发地址列表
   * @return 有权限返回true，否则返回false
   */
  public abstract boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, RMApp app, String remoteAddress,
      List<String> forwardedAddresses);

  /**
   * 检查调用用户将应用移动到目标队列的权限，用于应用迁移场景
   * @param callerUGI 调用者用户信息
   * @param acl 需要检查的队列ACL权限类型
   * @param app 待移动的应用
   * @param remoteAddress 远程客户端地址
   * @param forwardedAddresses 转发地址列表
   * @param targetQueue 目标队列名称
   * @return 目标队列存在且有权限返回true，否则返回false
   */
  public abstract boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, RMApp app, String remoteAddress,
      List<String> forwardedAddresses, String targetQueue);
}