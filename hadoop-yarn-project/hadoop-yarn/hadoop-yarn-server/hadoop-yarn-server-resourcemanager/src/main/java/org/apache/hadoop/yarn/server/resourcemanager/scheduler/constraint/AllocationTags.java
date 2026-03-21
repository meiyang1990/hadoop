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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.Set;

/**
 * 文件说明：YARN调度约束模块中，同一命名空间下分配标签的封装容器，用于存储应用分配标签及目标应用范围。
 * 核心作用：将分配标签集合与目标应用命名空间绑定，为节点调度约束提供查询依据。
 */
public final class AllocationTags {

  private TargetApplicationsNamespace ns;
  private Set<String> tags;
  private ApplicationId applicationId;

  /**
   * 构造函数，创建不包含当前应用ID的AllocationTags实例。
   * @param namespace 目标应用命名空间，指定标签作用的应用范围
   * @param allocationTags 分配标签集合
   */
  private AllocationTags(TargetApplicationsNamespace namespace,
      Set<String> allocationTags) {
    this.ns = namespace;
    this.tags = allocationTags;
  }

  /**
   * 构造函数，创建包含当前应用ID的AllocationTags实例。
   * @param namespace 目标应用命名空间，指定标签作用的应用范围
   * @param allocationTags 分配标签集合
   * @param currentAppId 当前查询所属的应用ID
   */
  private AllocationTags(TargetApplicationsNamespace namespace,
      Set<String> allocationTags, ApplicationId currentAppId) {
    this.ns = namespace;
    this.tags = allocationTags;
    this.applicationId = currentAppId;
  }

  /**
   * 获取当前标签集合所属的目标应用命名空间。
   * @return 目标应用命名空间实例
   */
  public TargetApplicationsNamespace getNamespace() {
    return this.ns;
  }

  /**
   * 获取当前查询所属的应用ID。
   * @return 当前应用ID，可能为null
   */
  public ApplicationId getCurrentApplicationId() {
    return this.applicationId;
  }

  /**
   * 获取当前命名空间下的分配标签集合。
   * @return 分配标签集合
   */
  public Set<String> getTags() {
    return this.tags;
  }

  /**
   * 创建仅针对单个指定应用的分配标签实例，仅用于测试。
   * @param appId 目标应用ID
   * @param tags 分配标签集合
   * @return 单个应用范围的分配标签实例
   */
  @VisibleForTesting
  public static AllocationTags createSingleAppAllocationTags(
      ApplicationId appId, Set<String> tags) {
    TargetApplicationsNamespace namespace =
        new TargetApplicationsNamespace.AppID(appId);
    return new AllocationTags(namespace, tags);
  }

  /**
   * 创建全局范围的分配标签实例，匹配所有应用，仅用于测试。
   * @param tags 分配标签集合
   * @return 全局范围的分配标签实例
   */
  @VisibleForTesting
  public static AllocationTags createGlobalAllocationTags(Set<String> tags) {
    TargetApplicationsNamespace namespace =
        new TargetApplicationsNamespace.All();
    return new AllocationTags(namespace, tags);
  }

  /**
   * 创建排除当前应用的分配标签实例，仅用于测试。
   * @param currentApp 当前应用ID
   * @param tags 分配标签集合
   * @return 排除当前应用范围的分配标签实例
   */
  @VisibleForTesting
  public static AllocationTags createOtherAppAllocationTags(
      ApplicationId currentApp, Set<String> tags) {
    TargetApplicationsNamespace namespace =
        new TargetApplicationsNamespace.NotSelf();
    return new AllocationTags(namespace, tags, currentApp);
  }

  /**
   * 根据字符串格式的命名空间解析创建分配标签实例。
   * @param currentApplicationId 当前查询所属的应用ID
   * @param namespaceString 字符串格式的命名空间定义
   * @param tags 分配标签集合
   * @return 解析后的分配标签实例
   * @throws InvalidAllocationTagsQueryException 命名空间解析失败时抛出
   */
  public static AllocationTags createAllocationTags(
      ApplicationId currentApplicationId, String namespaceString,
      Set<String> tags) throws InvalidAllocationTagsQueryException {
    TargetApplicationsNamespace namespace = TargetApplicationsNamespace
        .parse(namespaceString);
    return new AllocationTags(namespace, tags, currentApplicationId);
  }
}