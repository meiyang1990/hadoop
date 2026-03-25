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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * YARN调度约束处理中，存储目标应用集合及其标签信息，
 * 供命名空间约束评估时查询匹配的应用列表。
 * 该类由 {@link TargetApplicationsNamespace#evaluate(TargetApplications)} 使用，用于命名空间评估。
 */
public class TargetApplications {

  // 当前正在调度分配容器的应用ID
  private ApplicationId currentAppId;
  // 所有目标应用及其标签集合，key为应用ID，value为该应用绑定的标签列表
  private Map<ApplicationId, Set<String>> allApps;

  /**
   * 构造函数，仅传入应用ID不携带标签信息。
   * @param currentApplicationId 当前调度的应用ID
   * @param allApplicationIds 所有目标应用ID集合
   */
  public TargetApplications(ApplicationId currentApplicationId,
      Set<ApplicationId> allApplicationIds) {
    this.currentAppId = currentApplicationId;
    allApps = new HashMap<>();
    if (allApplicationIds != null) {
      allApplicationIds.forEach(appId ->
          allApps.put(appId, ImmutableSet.of()));
    }
  }

  /**
   * 构造函数，传入当前应用和所有应用的标签信息。
   * @param currentApplicationId 当前调度的应用ID
   * @param allApplicationIds 所有目标应用及其标签集合
   */
  public TargetApplications(ApplicationId currentApplicationId,
      Map<ApplicationId, Set<String>> allApplicationIds) {
    this.currentAppId = currentApplicationId;
    this.allApps = allApplicationIds;
  }

  /**
   * 获取当前正在调度的应用ID。
   * @return 当前应用ID
   */
  public ApplicationId getCurrentApplicationId() {
    return this.currentAppId;
  }

  /**
   * 获取所有目标应用的ID集合。
   * @return 所有目标应用ID集合，为空时返回空集合
   */
  public Set<ApplicationId> getAllApplicationIds() {
    return this.allApps == null ?
        ImmutableSet.of() : allApps.keySet();
  }

  /**
   * 获取除当前应用外的所有其他目标应用ID集合。
   * @return 其他应用ID集合，无其他应用时返回空集合
   */
  public Set<ApplicationId> getOtherApplicationIds() {
    if (getAllApplicationIds() == null
        || getAllApplicationIds().isEmpty()) {
      return ImmutableSet.of();
    }
    return getAllApplicationIds()
        .stream()
        .filter(appId -> !appId.equals(getCurrentApplicationId()))
        .collect(Collectors.toSet());
  }

  /**
   * 根据标签查询所有带有该标签的应用ID集合。
   * @param applicationTag 应用标签
   * @return 匹配该标签的所有应用ID集合，无匹配时返回空集合
   */
  public Set<ApplicationId> getApplicationIdsByTag(String applicationTag) {
    Set<ApplicationId> result = new HashSet<>();
    if (Strings.isNullOrEmpty(applicationTag)
        || this.allApps == null) {
      return result;
    }

    for (Map.Entry<ApplicationId, Set<String>> app
        : this.allApps.entrySet()) {
      if (app.getValue() != null
          && app.getValue().contains(applicationTag)) {
        result.add(app.getKey());
      }
    }

    return result;
  }
}