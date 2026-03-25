// 这个文件已经全部加上中文注释
/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;

/**
 * YARN 容器放置约束管理器接口，负责存储和管理应用级、全局级的容器放置约束规则。
 * 为YARN调度器提供统一的约束查询入口，支持应用级和集群管理员全局配置的放置约束。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface PlacementConstraintManager {

  /**
   * 注册整个应用的所有放置约束规则。
   *
   * @param appId 应用ID
   * @param constraintMap 该应用的分配标签集合到对应放置约束的映射
   */
  void registerApplication(ApplicationId appId,
      Map<Set<String>, PlacementConstraint> constraintMap);

  /**
   * 为指定应用和指定分配标签集合添加一个放置约束规则。
   * 该约束会被携带对应标签的调度请求使用。
   * TODO: 当前仅支持替换，后续需要支持约束合并
   *
   * @param appId 应用ID
   * @param sourceTags 触发该约束的分配标签集合
   * @param placementConstraint 要添加的放置约束
   * @param replace 是否替换已存在的同标签约束
   */
  void addConstraint(ApplicationId appId, Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace);

  /**
   * 添加全局级放置约束规则，整个集群所有应用生效。
   * 这类约束由集群管理员配置添加。
   * TODO: 当前仅支持替换，后续需要支持约束合并
   *
   * @param sourceTags 触发该约束的分配标签集合
   * @param placementConstraint 要添加的放置约束
   * @param replace 是否替换已存在的同标签约束
   */
  void addGlobalConstraint(Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace);

  /**
   * 获取指定应用的所有放置约束规则，包含对应触发的分配标签集合。
   *
   * @param appId 应用ID
   * @return 该应用的分配标签集合到约束的映射
   */
  Map<Set<String>, PlacementConstraint> getConstraints(ApplicationId appId);

  /**
   * 根据应用ID和分配标签集合获取对应的放置约束规则。
   *
   * @param appId 应用ID
   * @param sourceTags 触发约束的分配标签集合
   * @return 对应放置约束
   */
  PlacementConstraint getConstraint(ApplicationId appId,
      Set<String> sourceTags);

  /**
   * 根据分配标签集合获取对应的全局放置约束规则。
   *
   * @param sourceTags 触发约束的分配标签集合
   * @return 对应全局放置约束
   */
  PlacementConstraint getGlobalConstraint(Set<String> sourceTags);

  /**
   * 合并调度请求级、应用级、全局级三个层级的约束，返回合并后的最终放置约束。
   *
   * @param applicationId 应用ID
   * @param sourceTags 触发约束的源分配标签集合
   * @param schedulingRequestConstraint 调度请求级的放置约束
   * @return 合并后的最终放置约束
   */
  PlacementConstraint getMultilevelConstraint(ApplicationId applicationId,
      Set<String> sourceTags, PlacementConstraint schedulingRequestConstraint);

  /**
   * 注销应用，移除该应用所有的放置约束规则。
   *
   * @param appId 要注销的应用ID
   */
  void unregisterApplication(ApplicationId appId);

  /**
   * 移除指定分配标签对应的全局放置约束规则。
   *
   * @param sourceTags 对应分配标签集合
   */
  void removeGlobalConstraint(Set<String> sourceTags);

  /**
   * 获取当前已注册的应用数量。
   *
   * @return 已注册应用数量
   */
  int getNumRegisteredApplications();

  /**
   * 获取当前已注册的全局约束数量。
   *
   * @return 全局约束数量
   */
  int getNumGlobalConstraints();

  /**
   * 校验放置约束和对应的触发分配标签是否合法。
   *
   * @param sourceTags 关联的分配标签集合
   * @param placementConstraint 待校验的放置约束
   * @return 合法返回true，否则返回false
   */
  default boolean validateConstraint(Set<String> sourceTags,
      PlacementConstraint placementConstraint) {
    return true;
  }

}