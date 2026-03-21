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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.LocalityAppPlacementAllocator;

/**
 * 应用调度配置类，保存应用容器分配计算所需的所有调度环境配置项名称
 */
public class ApplicationSchedulingConfig {
  @InterfaceAudience.Private
  // 应用分配器类型环境变量名称
  public static final String ENV_APPLICATION_PLACEMENT_TYPE_CLASS =
      "APPLICATION_PLACEMENT_TYPE_CLASS";

  @InterfaceAudience.Private
  // 默认应用分配器实现类，使用基于本地性的分配策略
  public static final Class<? extends AppPlacementAllocator>
      DEFAULT_APPLICATION_PLACEMENT_TYPE_CLASS = LocalityAppPlacementAllocator.class;

  @InterfaceAudience.Private
  // 多节点排序策略环境变量名称
  public static final String ENV_MULTI_NODE_SORTING_POLICY_CLASS =
      "MULTI_NODE_SORTING_POLICY_CLASS";
}