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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：队列ACL权限检查管理器的通用实现，默认将权限检查委托给资源调度器完成
 */
public class GenericQueueACLsManager extends QueueACLsManager {

  /** 日志实例 */
  private static final Logger LOG = LoggerFactory
      .getLogger(GenericQueueACLsManager.class);

  /**
   * 构造通用队列ACL权限管理器
   * @param scheduler 资源调度器实例
   * @param conf 配置对象
   */
  public GenericQueueACLsManager(ResourceScheduler scheduler,
      Configuration conf) {
    super(scheduler, conf);
  }

  /**
   * 检查应用当前队列的访问权限
   * @param callerUGI 调用者用户信息
   * @param acl 待检查的队列权限类型
   * @param app 待访问的YARN应用
   * @param remoteAddress 远程客户端地址
   * @param forwardedAddresses 转发地址列表
   * @return 是否拥有访问权限
   */
  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses) {
    // 委托资源调度器执行权限检查
    return scheduler.checkAccess(callerUGI, acl, app.getQueue());
  }

  /**
   * 检查目标队列的访问权限，用于队列移动等跨队列操作
   * @param callerUGI 调用者用户信息
   * @param acl 待检查的队列权限类型
   * @param app 待访问的YARN应用
   * @param remoteAddress 远程客户端地址
   * @param forwardedAddresses 转发地址列表
   * @param targetQueue 目标队列名称
   * @return 是否拥有访问权限
   */
  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl,
      RMApp app, String remoteAddress, List<String> forwardedAddresses,
      String targetQueue) {
    // 委托资源调度器执行权限检查
    return scheduler.checkAccess(callerUGI, acl, targetQueue);
  }
}