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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import java.util.HashMap;
import java.util.Map;

/**
 * YARN资源预留访问控制管理器抽象基类，用于检查用户对指定队列预留操作的访问权限。
 * 基于ReservationACL定义不同操作的权限控制，按队列区分权限配置。
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public abstract class ReservationsACLsManager {
  // 是否开启预留ACL权限控制
  private boolean isReservationACLsEnable;
  // 按队列存储ACL配置：队列名 -> (预留ACL类型 -> 访问控制列表)
  Map<String, Map<ReservationACL, AccessControlList>> reservationAcls =
      new HashMap<>();

  /**
   * 构造函数，从配置中初始化预留ACL开关状态。
   * @param conf YARN配置对象
   * @throws YarnException 配置解析异常
   */
  public ReservationsACLsManager(Configuration conf) throws YarnException {
    // 只有当全局ACL开关和预留ACL开关同时开启时，才启用权限控制
    this.isReservationACLsEnable = conf.getBoolean(
        YarnConfiguration.YARN_RESERVATION_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_RESERVATION_ACL_ENABLE)
        && conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
            YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
  }

  /**
   * 检查调用用户是否拥有指定队列对应预留操作的访问权限。
   * @param callerUGI 调用者用户信息
   * @param acl 预留操作ACL类型
   * @param queueName 目标队列名称
   * @return 有权限返回true，无权限返回false
   */
  public boolean checkAccess(UserGroupInformation callerUGI,
      ReservationACL acl, String queueName) {
    // 如果未开启ACL控制，直接允许所有访问
    if (!isReservationACLsEnable) {
      return true;
    }

    // 检查队列是否存在ACL配置
    if (this.reservationAcls.containsKey(queueName)) {
      Map<ReservationACL, AccessControlList> acls = this.reservationAcls.get(
              queueName);
      // 当前操作存在ACL配置，检查用户权限
      if (acls != null && acls.containsKey(acl)) {
        return acls.get(acl).isUserAllowed(callerUGI);
      } else {
        // 队列未配置当前操作的ACL，默认允许访问
        return true;
      }
    }

    // 队列不存在任何ACL配置，默认拒绝访问
    return false;
  }
}