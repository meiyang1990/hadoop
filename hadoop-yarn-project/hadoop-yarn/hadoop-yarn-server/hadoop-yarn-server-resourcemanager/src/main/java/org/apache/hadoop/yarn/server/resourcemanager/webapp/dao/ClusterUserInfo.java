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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 集群用户信息数据访问对象，用于YARN Web UI展示当前认证用户信息。
 * YARN UI本身没有集中登录机制，请求经过代理转发后，只有ResourceManager能获取真实发起请求的用户。
 * 该DAO用于返回RM启动用户和当前请求认证用户信息，在前端展示真实认证用户避免混淆。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@InterfaceStability.Unstable
public class ClusterUserInfo {

    // ResourceManager自身启动登录用户名
    protected String rmLoginUser;
    // 当前请求发起用户的用户名
    protected String requestedUser;

  // 联邦集群环境下子集群ID
  private String subClusterId;

    public ClusterUserInfo() {
    }

    /**
     * 构造集群用户信息对象
     * @param rm ResourceManager实例
     * @param ugi 当前请求的用户信息
     */
    public ClusterUserInfo(ResourceManager rm, UserGroupInformation ugi) {
        this.rmLoginUser = rm.getRMLoginUser();
        if (ugi != null) {
            this.requestedUser = ugi.getShortUserName();
        } else {
            this.requestedUser = "UNKNOWN_USER";
        }
    }

    public String getRmLoginUser() {
        return rmLoginUser;
    }

    public String getRequestedUser() {
        return requestedUser;
    }

  public String getSubClusterId() {
    return subClusterId;
  }

  public void setSubClusterId(String subClusterId) {
    this.subClusterId = subClusterId;
  }
}