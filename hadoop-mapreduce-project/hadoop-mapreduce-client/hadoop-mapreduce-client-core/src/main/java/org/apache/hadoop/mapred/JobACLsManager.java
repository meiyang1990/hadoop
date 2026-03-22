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
package org.apache.hadoop.mapred;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce作业访问权限控制管理器，负责检查用户对作业的操作权限，
 * 支持基于ACL的权限管理，区分管理员、作业所有者和普通用户的权限。
 */
@InterfaceAudience.Private
public class JobACLsManager {

  static final Logger LOG = LoggerFactory.getLogger(JobACLsManager.class);
  Configuration conf;
  private final AccessControlList adminAcl;

  /**
   * 构造作业权限管理器，从配置中初始化MapReduce管理员ACL
   * @param conf 配置对象
   */
  public JobACLsManager(Configuration conf) {
    adminAcl = new AccessControlList(conf.get(MRConfig.MR_ADMINS, " "));
    this.conf = conf;
  }

  /**
   * 检查ACL权限控制功能是否已启用
   * @return true表示启用，false表示禁用
   */
  public boolean areACLsEnabled() {
    return conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
  }

  /**
   * Construct the jobACLs from the configuration so that they can be kept in
   * the memory. If authorization is disabled on the JT, nothing is constructed
   * and an empty map is returned.
   * 
   * @return JobACL to AccessControlList map.
   */
  public Map<JobACL, AccessControlList> constructJobACLs(Configuration conf) {

    Map<JobACL, AccessControlList> acls =
        new HashMap<JobACL, AccessControlList>();

    // Don't construct anything if authorization is disabled.
    if (!areACLsEnabled()) {
      return acls;
    }

    // 遍历所有作业权限类型，从配置中解析ACL并保存
    for (JobACL aclName : JobACL.values()) {
      String aclConfigName = aclName.getAclName();
      String aclConfigured = conf.get(aclConfigName);
      if (aclConfigured == null) {
        // If ACLs are not configured at all, we grant no access to anyone. So
        // jobOwner and cluster administrator _only_ can do 'stuff'
        aclConfigured = " ";
      }
      acls.put(aclName, new AccessControlList(aclConfigured));
    }
    return acls;
  }

  /**
    * 检查调用用户是否属于MapReduce集群管理员组
    * @param callerUGI 调用者用户信息
    * @return true表示用户是管理员
    */
   boolean isMRAdmin(UserGroupInformation callerUGI) {
     if (adminAcl.isUserAllowed(callerUGI)) {
       return true;
     }
     return false;
   }

  /**
   * If authorization is enabled, checks whether the user (in the callerUGI)
   * is authorized to perform the operation specified by 'jobOperation' on
   * the job by checking if the user is jobOwner or part of job ACL for the
   * specific job operation.
   * <ul>
   * <li>The owner of the job can do any operation on the job</li>
   * <li>For all other users/groups job-acls are checked</li>
   * </ul>
   * @param callerUGI 调用者用户信息
   * @param jobOperation 要执行的作业操作类型
   * @param jobOwner 作业所有者用户名
   * @param jobACL 对应操作的访问控制列表
   * @return true表示允许访问，false表示拒绝
   */
  public boolean checkAccess(UserGroupInformation callerUGI,
      JobACL jobOperation, String jobOwner, AccessControlList jobACL) {

    // 调试模式下记录访问检查日志
    if (LOG.isDebugEnabled()) {
      LOG.debug("checkAccess job acls, jobOwner: " + jobOwner + " jobacl: "
          + jobOperation.toString() + " user: " + callerUGI.getShortUserName());
    }
    String user = callerUGI.getShortUserName();
    // ACL未启用时直接允许所有访问
    if (!areACLsEnabled()) {
      return true;
    }

    // 允许三种情况通过检查：MapReduce管理员、作业所有者、ACL列表中允许的用户
    if (isMRAdmin(callerUGI)
        || user.equals(jobOwner)
        || (null != jobACL && jobACL.isUserAllowed(callerUGI))) {
      return true;
    }

    return false;
  }
}