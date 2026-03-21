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

package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * 映射规则验证工具类，为容量调度器队列自动创建提供路径验证逻辑。
 * 本类是临时拆分提取，计划后续合并到CapacityScheduler或CapacitySchedulerQueueManager中。
 */
public final class MappingRuleValidationHelper {
  /**
   * 队列路径验证结果枚举，定义所有可能的验证结果。
   */
  public enum ValidationResult {
    /** 可创建新队列 */
    CREATABLE,
    /** 队列已存在 */
    QUEUE_EXISTS,
    /** 未提供父队列路径 */
    NO_PARENT_PROVIDED,
    /** 父队列不支持动态创建 */
    NO_DYNAMIC_PARENT,
    /** 父队列路径不明确 */
    AMBIGUOUS_PARENT,
    /** 队列路径不明确 */
    AMBIGUOUS_QUEUE,
    /** 路径为空 */
    EMPTY_PATH
  }

  /**
   * 工具类私有构造方法，禁止实例化。
   */
  private MappingRuleValidationHelper() {

  }

  /**
   * 标准化队列路径根节点，将路径根替换为队列管理器中实际存储的完整路径。
   * 用于处理路径根的歧义，确保路径格式统一。
   * @param queueManager 容量调度器队列管理器
   * @param fullPath 待标准化的完整队列路径
   * @return 标准化后的完整队列路径
   * @throws YarnException 当路径根不存在或存在歧义时抛出异常
   */
  public static String normalizeQueuePathRoot(
      CapacitySchedulerQueueManager queueManager, String fullPath)
      throws YarnException {
    // 将完整路径按点分割为分段
    ArrayList<String> parts = new ArrayList<>();
    Collections.addAll(parts, fullPath.split("\\."));

    // 获取路径根节点，查找对应队列
    String pathRoot = parts.get(0);
    CSQueue pathRootQueue = queueManager.getQueue(pathRoot);
    if (pathRootQueue == null) {
      if (queueManager.isAmbiguous(pathRoot)) {
        throw new YarnException("Path root '" + pathRoot +
            "' is ambiguous. Path '" + fullPath + "' is invalid");
      } else {
        throw new YarnException("Path root '" + pathRoot +
            "' does not exist. Path '" + fullPath + "' is invalid");
      }
    }

    // 替换根节点为实际队列路径，拼接返回标准化路径
    parts.set(0, pathRootQueue.getQueuePath());
    return String.join(DOT, parts);
  }

  /**
   * 验证队列路径是否满足自动创建队列的条件。
   * 检查路径合法性、父队列存在性和动态创建权限。
   * @param queueManager 容量调度器队列管理器
   * @param path 待验证的目标队列路径
   * @return 验证结果枚举
   */
  public static ValidationResult validateQueuePathAutoCreation(
      CapacitySchedulerQueueManager queueManager, String path) {
    // 检查路径是否为空
    if (path == null || path.isEmpty()) {
      return ValidationResult.EMPTY_PATH;
    }

    // 检查队列是否已存在
    if (queueManager.getQueue(path) != null) {
      return ValidationResult.QUEUE_EXISTS;
    }

    // 检查当前路径是否存在歧义
    if (queueManager.isAmbiguous(path)) {
      return ValidationResult.AMBIGUOUS_QUEUE;
    }

    // 将路径按点分割为分段
    ArrayList<String> parts = new ArrayList<>();
    Collections.addAll(parts, path.split("\\."));

    // 移除叶子队列名，得到父队列路径
    parts.remove(parts.size() - 1);
    String parentPath = parts.size() >= 1 ? String.join(".", parts) : "";
    // 再移除父队列名，得到祖父队列路径
    parts.remove(parts.size() - 1);
    String grandParentPath = parts.size() >= 1 ? String.join(".", parts) : "";

    // 检查父队列路径是否为空
    if (parentPath.isEmpty()) {
      return ValidationResult.NO_PARENT_PROVIDED;
    }

    // 检查父队列路径是否存在歧义
    if (queueManager.isAmbiguous(parentPath)) {
      return ValidationResult.AMBIGUOUS_PARENT;
    }
    // 获取父队列对象
    CSQueue parentQueue = queueManager.getQueue(parentPath);
    if (parentQueue == null) {
      // 父队列不存在，检查是否有祖父队列
      if (grandParentPath.isEmpty()) {
        return ValidationResult.NO_PARENT_PROVIDED;
      }

      // 检查祖父队列路径是否存在歧义
      if (queueManager.isAmbiguous(grandParentPath)) {
        return ValidationResult.AMBIGUOUS_PARENT;
      }
      // 父队列不存在时，检查祖父是否允许动态创建子队列
      CSQueue grandParentQueue = queueManager.getQueue(grandParentPath);
      if (grandParentQueue != null && grandParentQueue instanceof AbstractParentQueue &&
          ((AbstractParentQueue)grandParentQueue).isEligibleForAutoQueueCreation()) {
        // 祖父允许动态创建，可以创建父队列和当前叶子队列
        return ValidationResult.CREATABLE;
      }

      return ValidationResult.NO_DYNAMIC_PARENT;
    }

    // 父队列已存在，检查父队列是否允许创建子队列
    if (parentQueue instanceof ManagedParentQueue) {
      // 传统ManagedParentQueue默认允许自动创建
      return ValidationResult.CREATABLE;
    }
    if (parentQueue instanceof ParentQueue) {
      // 新式ParentQueue需要检查是否允许自动创建
      if (((ParentQueue)parentQueue).isEligibleForAutoQueueCreation()) {
        return ValidationResult.CREATABLE;
      }
    }
    // 父队列不允许自动创建队列
    return ValidationResult.NO_DYNAMIC_PARENT;
  }
}