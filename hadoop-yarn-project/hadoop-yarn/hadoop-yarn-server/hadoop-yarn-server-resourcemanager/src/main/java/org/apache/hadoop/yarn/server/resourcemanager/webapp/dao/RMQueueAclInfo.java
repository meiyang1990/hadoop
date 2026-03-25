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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN RM队列访问ACL检查结果数据对象，用于Web API返回权限检查结果信息
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RMQueueAclInfo {
  // 是否允许访问队列
  private Boolean allowed;
  // 提交访问请求的用户
  private String user;
  // 诊断信息，用于说明拒绝访问的原因
  private String diagnostics;
  // 目标子集群ID（联邦场景使用）
  private String subClusterId;

  public RMQueueAclInfo() {
    
  }

  /**
   * 构造队列ACL检查结果对象
   * @param allowed 是否允许访问
   * @param user 请求访问的用户
   * @param diagnostics 诊断信息
   */
  public RMQueueAclInfo(boolean allowed, String user, String diagnostics) {
    this.allowed = allowed;
    this.user = user;
    this.diagnostics = diagnostics;
  }

  /**
   * 构造支持联邦子集群的队列ACL检查结果对象
   * @param allowed 是否允许访问
   * @param user 请求访问的用户
   * @param diagnostics 诊断信息
   * @param subClusterId 目标子集群ID
   */
  public RMQueueAclInfo(boolean allowed, String user, String diagnostics, String subClusterId) {
    this.allowed = allowed;
    this.user = user;
    this.diagnostics = diagnostics;
    this.subClusterId = subClusterId;
  }

  public boolean isAllowed() {
    return allowed;
  }

  public void setAllowed(boolean allowed) {
    this.allowed = allowed;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(String diagnostics) {
    this.diagnostics = diagnostics;
  }

  public String getSubClusterId() {
    return subClusterId;
  }

  public void setSubClusterId(String subClusterId) {
    this.subClusterId = subClusterId;
  }
}