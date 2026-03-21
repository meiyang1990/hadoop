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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourcesInfo;

/**
 * 容量调度器中单个用户的资源使用信息，用于REST API暴露用户调度数据，
 * 包含用户名、资源使用量、应用数量、资源限额等信息。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class UserInfo {
  // 用户名
  protected String  username;
  // 用户已使用的资源信息
  protected ResourceInfo resourcesUsed;
  // 用户待调度应用数量
  protected int numPendingApplications;
  // 用户活跃应用数量
  protected int numActiveApplications;
  // 用户已使用的ApplicationMaster资源
  protected ResourceInfo AMResourceUsed;
  // 用户的资源限额
  protected ResourceInfo userResourceLimit;
  // 用户完整资源使用统计信息
  protected ResourcesInfo resources;
  // 用户调度权重
  private float userWeight;
  // 用户是否处于活跃状态
  private boolean isActive;

  UserInfo() {}

  /**
   * 构造用户调度信息对象，封装用户资源使用和应用状态数据。
   * @param username 用户名
   * @param resUsed 用户已使用的总资源
   * @param activeApps 用户活跃应用数量
   * @param pendingApps 用户待调度应用数量
   * @param amResUsed 用户已使用的AM资源
   * @param resourceLimit 用户资源限额
   * @param resourceUsage 用户完整资源使用统计
   * @param weight 用户调度权重
   * @param isActive 用户是否活跃
   */
  UserInfo(String username, Resource resUsed, int activeApps, int pendingApps,
      Resource amResUsed, Resource resourceLimit, ResourceUsage resourceUsage,
      float weight, boolean isActive) {
    this.username = username;
    this.resourcesUsed = new ResourceInfo(resUsed);
    this.numActiveApplications = activeApps;
    this.numPendingApplications = pendingApps;
    this.AMResourceUsed = new ResourceInfo(amResUsed);
    this.userResourceLimit = new ResourceInfo(resourceLimit);
    this.resources = new ResourcesInfo(resourceUsage);
    this.userWeight = weight;
    this.isActive = isActive;
  }

  public String getUsername() {
    return username;
  }

  public ResourceInfo getResourcesUsed() {
    return resourcesUsed;
  }

  public int getNumPendingApplications() {
    return numPendingApplications;
  }

  public int getNumActiveApplications() {
    return numActiveApplications;
  }

  public ResourceInfo getAMResourcesUsed() {
    return AMResourceUsed;
  }

  public ResourceInfo getUserResourceLimit() {
    return userResourceLimit;
  }

  public ResourcesInfo getResourceUsageInfo() {
    return resources;
  }

  public float getUserWeight() {
    return userWeight;
  }

  public boolean getIsActive() {
    return isActive;
  }
}