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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ApplicationSchedulingConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

/**
 * 文件说明：YARN资源调度器应用位置分配器工厂，用于根据配置创建不同策略的应用位置分配器
 * 工厂类，用于构建各类应用位置分配策略实例
 */
@Public
@Unstable
public class ApplicationPlacementAllocatorFactory {

  /**
   * 根据请求的位置分配类型，获取对应的应用位置分配器实例
   *
   * @param appPlacementAllocatorName
   *          位置分配器实现类全类名
   * @param appSchedulingInfo 应用调度信息
   * @param schedulerRequestKey 调度请求唯一标识
   * @param rmContext ResourceManager上下文对象
   * @return 对应类型的应用位置分配器实例
   */
  public static AppPlacementAllocator<SchedulerNode> getAppPlacementAllocator(
      String appPlacementAllocatorName, AppSchedulingInfo appSchedulingInfo,
      SchedulerRequestKey schedulerRequestKey, RMContext rmContext) {
    Class<?> policyClass;
    try {
      // 分配器类名为空，使用默认位置分配器实现
      if (StringUtils.isEmpty(appPlacementAllocatorName)) {
        policyClass = ApplicationSchedulingConfig.DEFAULT_APPLICATION_PLACEMENT_TYPE_CLASS;
      } else {
        // 加载用户指定的分配器类
        policyClass = Class.forName(appPlacementAllocatorName);
      }
    } catch (ClassNotFoundException e) {
      // 找不到指定类，回退到默认分配器
      policyClass = ApplicationSchedulingConfig.DEFAULT_APPLICATION_PLACEMENT_TYPE_CLASS;
    }

    // 检查加载的类是否实现了AppPlacementAllocator接口，不满足则使用默认分配器
    if (!AppPlacementAllocator.class.isAssignableFrom(policyClass)) {
      policyClass = ApplicationSchedulingConfig.DEFAULT_APPLICATION_PLACEMENT_TYPE_CLASS;
    }

    @SuppressWarnings("unchecked")
    // 通过反射创建分配器实例
    AppPlacementAllocator<SchedulerNode> placementAllocatorInstance = (AppPlacementAllocator<SchedulerNode>) ReflectionUtils
        .newInstance(policyClass, null);
    // 初始化分配器，传入所需上下文信息
    placementAllocatorInstance.initialize(appSchedulingInfo,
        schedulerRequestKey, rmContext);
    // 返回初始化完成的分配器实例
    return placementAllocatorInstance;
  }
}