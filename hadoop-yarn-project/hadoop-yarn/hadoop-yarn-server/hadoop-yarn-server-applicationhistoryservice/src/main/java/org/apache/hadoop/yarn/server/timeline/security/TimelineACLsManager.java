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

package org.apache.hadoop.yarn.server.timeline.security;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.map.LRUMap;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.server.timeline.EntityIdentifier;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.util.StringHelper;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时间线服务访问控制列表管理器，负责检查时间线数据的实体级访问权限
 */
@Private
public class TimelineACLsManager {

  private static final Logger LOG = LoggerFactory.
      getLogger(TimelineACLsManager.class);
  /** 域访问条目缓存大小，使用LRU策略缓存 */
  private static final int DOMAIN_ACCESS_ENTRY_CACHE_SIZE = 100;

  private AdminACLsManager adminAclsManager;
  /** 域访问控制缓存，key为域ID，value为扩展访问控制对象 */
  private Map<String, AccessControlListExt> aclExts;
  /** 时间线存储层引用，用于获取域信息 */
  private TimelineStore store;

  @SuppressWarnings("unchecked")
  /**
   * 构造函数，初始化访问控制缓存和管理员ACL管理器
   * @param conf Yarn配置对象
   */
  public TimelineACLsManager(Configuration conf) {
    this.adminAclsManager = new AdminACLsManager(conf);
    aclExts = Collections.synchronizedMap(
        new LRUMap(DOMAIN_ACCESS_ENTRY_CACHE_SIZE));
  }

  /**
   * 设置时间线存储对象，用于后续获取域信息
   * @param store 时间线存储实例
   */
  public void setTimelineStore(TimelineStore store) {
    this.store = store;
  }

  /**
   * 从时间线存储加载指定域的访问控制信息
   * @param domainId 域ID
   * @return 扩展访问控制对象，不存在则返回null
   * @throws IO异常
   */
  private AccessControlListExt loadDomainFromTimelineStore(
      String domainId) throws IOException {
    if (store == null) {
      return null;
    }
    TimelineDomain domain = store.getDomain(domainId);
    if (domain == null) {
      return null;
    } else {
      return putDomainIntoCache(domain);
    }
  }

  /**
   * 如果域已在缓存中，替换缓存中的访问控制信息
   * @param domain 时间线域对象
   */
  public void replaceIfExist(TimelineDomain domain) {
    if (aclExts.containsKey(domain.getId())) {
      putDomainIntoCache(domain);
    }
  }

  /**
   * 将域对象转换为扩展访问控制对象并放入缓存
   * @param domain 时间线域对象
   * @return 转换后的扩展访问控制对象
   */
  private AccessControlListExt putDomainIntoCache(
      TimelineDomain domain) {
    Map<ApplicationAccessType, AccessControlList> acls
    = new HashMap<ApplicationAccessType, AccessControlList>(2);
    // 构造读权限访问控制列表
    acls.put(ApplicationAccessType.VIEW_APP,
        new AccessControlList(StringHelper.cjoin(domain.getReaders())));
    // 构造写权限访问控制列表
    acls.put(ApplicationAccessType.MODIFY_APP,
        new AccessControlList(StringHelper.cjoin(domain.getWriters())));
    AccessControlListExt aclExt =
        new AccessControlListExt(domain.getOwner(), acls);
    aclExts.put(domain.getId(), aclExt);
    return aclExt;
  }

  /**
   * 检查当前用户是否有权限访问指定时间线实体
   * @param callerUGI 调用者用户信息
   * @param applicationAccessType 访问类型（读/写）
   * @param entity 待访问的时间线实体
   * @return 有权限返回true，否则返回false
   * @throws YarnException 当实体所属域不存在时抛出
   * @throws IO异常
   */
  public boolean checkAccess(UserGroupInformation callerUGI,
      ApplicationAccessType applicationAccessType,
      TimelineEntity entity) throws YarnException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Verifying the access of "
          + (callerUGI == null ? null : callerUGI.getShortUserName())
          + " on the timeline entity "
          + new EntityIdentifier(entity.getEntityId(), entity.getEntityType()));
    }

    // 如果ACL未开启，直接允许访问
    if (!adminAclsManager.areACLsEnabled()) {
      return true;
    }

    // 从缓存获取域访问控制信息
    AccessControlListExt aclExt = aclExts.get(entity.getDomainId());
    // 缓存未命中，从存储加载
    if (aclExt == null) {
      aclExt = loadDomainFromTimelineStore(entity.getDomainId());
    }
    // 加载不到域信息，抛出异常
    if (aclExt == null) {
      throw new YarnException("Domain information of the timeline entity "
          + new EntityIdentifier(entity.getEntityId(), entity.getEntityType())
          + " doesn't exist.");
    }
    String owner = aclExt.owner;
    AccessControlList domainACL = aclExt.acls.get(applicationAccessType);
    // ACL不存在，使用默认ACL
    if (domainACL == null) {
      LOG.debug("ACL not found for access-type {} for domain {} owned by {}."
          + " Using default [{}]", applicationAccessType,
          entity.getDomainId(), owner, YarnConfiguration.DEFAULT_YARN_APP_ACL);
      domainACL =
          new AccessControlList(YarnConfiguration.DEFAULT_YARN_APP_ACL);
    }

    // 满足任意条件则允许访问：管理员/域所有者/ACL允许当前用户
    if (callerUGI != null
        && (adminAclsManager.isAdmin(callerUGI) ||
            callerUGI.getShortUserName().equals(owner) ||
            domainACL.isUserAllowed(callerUGI))) {
      return true;
    }
    return false;
  }

  /**
   * 检查当前用户是否有权限修改指定时间线域
   * @param callerUGI 调用者用户信息
   * @param domain 待访问的时间线域
   * @return 有权限返回true，否则返回false
   * @throws YarnException 当域所有者信息损坏时抛出
   * @throws IO异常
   */
  public boolean checkAccess(UserGroupInformation callerUGI,
      TimelineDomain domain) throws YarnException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Verifying the access of "
          + (callerUGI == null ? null : callerUGI.getShortUserName())
          + " on the timeline domain " + domain);
    }

    // 如果ACL未开启，直接允许访问
    if (!adminAclsManager.areACLsEnabled()) {
      return true;
    }

    String owner = domain.getOwner();
    // 所有者信息为空，抛出异常
    if (owner == null || owner.length() == 0) {
      throw new YarnException("Owner information of the timeline domain "
          + domain.getId() + " is corrupted.");
    }
    // 满足任意条件则允许访问：管理员/域所有者
    if (callerUGI != null
        && (adminAclsManager.isAdmin(callerUGI) ||
            callerUGI.getShortUserName().equals(owner))) {
      return true;
    }
    return false;
  }

  @Private
  @VisibleForTesting
  /**
   * 替换管理员ACL管理器，仅用于测试
   * @param adminAclsManager 新的管理员ACL管理器
   * @return 旧的管理员ACL管理器
   */
  public AdminACLsManager
      setAdminACLsManager(AdminACLsManager adminAclsManager) {
    AdminACLsManager oldAdminACLsManager = this.adminAclsManager;
    this.adminAclsManager = adminAclsManager;
    return oldAdminACLsManager;
  }

  /**
   * 扩展访问控制列表，封装域所有者和各类访问权限的ACL
   */
  private static class AccessControlListExt {
    private String owner;
    private Map<ApplicationAccessType, AccessControlList> acls;

    public AccessControlListExt(
        String owner, Map<ApplicationAccessType, AccessControlList> acls) {
      this.owner = owner;
      this.acls = acls;
    }
  }
}