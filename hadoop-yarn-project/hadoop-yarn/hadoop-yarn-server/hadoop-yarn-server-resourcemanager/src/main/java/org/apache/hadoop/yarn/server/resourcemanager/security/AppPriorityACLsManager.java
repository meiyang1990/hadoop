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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AppPriorityACLGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 应用优先级ACL权限管理器，负责存储和检查用户提交应用优先级的权限。
 * 控制不同用户/组在指定队列中可以提交的最高应用优先级，并提供默认优先级映射。
 */
public class AppPriorityACLsManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(AppPriorityACLsManager.class);

  /*
   * 内部类，存储每个优先级ACL配置信息，在应用提交时用于权限检查。
   */
  private static class PriorityACL {
    private Priority priority;
    private Priority defaultPriority;
    private AccessControlList acl;

    /**
     * 构造单个优先级ACL配置项。
     * @param priority 允许的最高优先级
     * @param defaultPriority 该ACL组对应用户的默认优先级
     * @param acl 允许访问此优先级范围的用户/组ACL
     */
    PriorityACL(Priority priority, Priority defaultPriority,
        AccessControlList acl) {
      this.setPriority(priority);
      this.setDefaultPriority(defaultPriority);
      this.setAcl(acl);
    }

    public Priority getPriority() {
      return priority;
    }

    public void setPriority(Priority maxPriority) {
      this.priority = maxPriority;
    }

    public Priority getDefaultPriority() {
      return defaultPriority;
    }

    public void setDefaultPriority(Priority defaultPriority) {
      this.defaultPriority = defaultPriority;
    }

    public AccessControlList getAcl() {
      return acl;
    }

    public void setAcl(AccessControlList acl) {
      this.acl = acl;
    }
  }

  // ACL权限检查总开关
  private boolean isACLsEnable;
  // 按队列存储所有优先级ACL配置，key为队列名，value为该队列的优先级ACL列表
  private final ConcurrentMap<String, List<PriorityACL>> allAcls =
      new ConcurrentHashMap<>();

  /**
   * 构造应用优先级ACL管理器，从配置读取ACL总开关。
   * @param conf YARN配置
   */
  public AppPriorityACLsManager(Configuration conf) {
    this.isACLsEnable = conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
  }

  /**
   * 刷新配置时清空指定队列的优先级ACL配置。
   *
   * @param queueName
   *          队列名称
   */
  public void clearPriorityACLs(String queueName) {
    allAcls.remove(queueName);
  }

  /**
   * 将指定队列的优先级ACL组列表存储到管理器中。
   *
   * @param priorityACLGroups
   *          优先级ACL组列表
   * @param queueName
   *          ACL组关联的队列名称
   */
  public void addPrioirityACLs(List<AppPriorityACLGroup> priorityACLGroups,
      String queueName) {

    List<PriorityACL> priorityACL = allAcls.get(queueName);
    if (null == priorityACL) {
      priorityACL = new ArrayList<PriorityACL>();
      allAcls.put(queueName, priorityACL);
    }

    // 按优先级升序排序，确保低优先级ACL排在前面
    Collections.sort(priorityACLGroups);

    // 遍历转换并存储所有优先级ACL配置
    for (AppPriorityACLGroup priorityACLGroup : priorityACLGroups) {
      priorityACL.add(new PriorityACL(priorityACLGroup.getMaxPriority(),
          priorityACLGroup.getDefaultPriority(),
          priorityACLGroup.getACLList()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Priority ACL group added: max-priority - "
            + priorityACLGroup.getMaxPriority() + "default-priority - "
            + priorityACLGroup.getDefaultPriority());
      }
    }
  }

  /**
   * 检查提交用户是否有权限在指定队列以指定优先级提交应用。
   *
   * @param callerUGI
   *          提交应用的用户信息
   * @param queueName
   *          应用提交到的目标队列
   * @param submittedPriority
   *          申请提交的应用优先级
   * @return 有权限返回true，否则返回false
   */
  public boolean checkAccess(UserGroupInformation callerUGI, String queueName,
      Priority submittedPriority) {
    // ACL未开启，直接放行
    if (!isACLsEnable) {
      return true;
    }

    // 获取队列ACL配置，无配置直接放行
    List<PriorityACL> acls = allAcls.get(queueName);
    if (acls == null || acls.isEmpty()) {
      return true;
    }

    // 查找匹配当前用户和提交优先级的ACL配置
    PriorityACL approvedPriorityACL = getMappedPriorityAclForUGI(acls,
        callerUGI, submittedPriority);
    if (approvedPriorityACL == null) {
      return false;
    }

    return true;
  }

  /**
   * 获取对应用户在指定队列的默认优先级，当应用未指定优先级时使用。
   *
   * @param queueName
   *          提交的目标队列
   * @param user
   *          提交应用的用户
   * @return 对应该用户的默认优先级，无匹配则返回null
   */
  public Priority getDefaultPriority(String queueName,
      UserGroupInformation user) {
    if (!isACLsEnable) {
      return null;
    }

    List<PriorityACL> acls = allAcls.get(queueName);
    if (acls == null || acls.isEmpty()) {
      return null;
    }

    // 查找匹配当前用户的ACL配置
    PriorityACL approvedPriorityACL = getMappedPriorityAclForUGI(acls, user,
        null);
    if (approvedPriorityACL == null) {
      return null;
    }

    // 返回新的默认优先级实例
    Priority defaultPriority = Priority
        .newInstance(approvedPriorityACL.getDefaultPriority().getPriority());
    return defaultPriority;
  }

  /**
   * 按优先级升序遍历ACL列表，找到匹配当前用户的最高允许优先级配置。
   * @param acls 队列的优先级ACL列表（已按优先级升序排序）
   * @param user 当前用户信息
   * @param submittedPriority 用户提交的优先级，null表示仅查询默认优先级
   * @return 匹配的ACL配置，无匹配返回null
   */
  private PriorityACL getMappedPriorityAclForUGI(List<PriorityACL> acls ,
      UserGroupInformation user, Priority submittedPriority) {

    // 从低优先级到高优先级遍历，最后一个匹配用户ACL的配置为有效配置
    PriorityACL selectedAcl = null;
    for (PriorityACL entry : acls) {
      AccessControlList list = entry.getAcl();

      // 检查用户是否在当前ACL组中
      if (list.isUserAllowed(user)) {
        selectedAcl = entry;

        // 如果指定了提交优先级，需要进一步检查是否不超过当前ACL允许的最高优先级
        // 由于列表已升序排序，第一个满足优先级要求的就是最高允许优先级
        if (submittedPriority != null) {
          selectedAcl = null;
          if (submittedPriority.getPriority() <= entry.getPriority()
              .getPriority()) {
            return entry;
          }
        }
      }
    }
    // 查询默认优先级场景返回最后一个匹配的ACL配置，即用户允许的最高优先级对应的默认值
    return selectedAcl;
  }
}