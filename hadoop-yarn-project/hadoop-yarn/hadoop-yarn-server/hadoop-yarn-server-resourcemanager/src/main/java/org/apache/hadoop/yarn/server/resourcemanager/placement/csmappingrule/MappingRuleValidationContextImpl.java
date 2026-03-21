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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;

import java.util.*;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * 容量调度器映射规则验证上下文实现类，保存验证所需上下文信息，提供队列路径验证能力
 */
public class MappingRuleValidationContextImpl
    implements MappingRuleValidationContext {
  /**
   * We store all known variables in this set.
   */
  private Set<String> knownVariables = Sets.newHashSet();

  /**
   * This set is to determine which variables are immutable.
   */
  private Set<String> immutableVariables = Sets.newHashSet();

  /**
   * For queue path validations we need an instance of the queue manager
   * to look up queues and their parents.
   */
  private final CapacitySchedulerQueueManager queueManager;

  /**
   * 构造方法，使用队列管理器初始化验证上下文
   * @param qm 容量调度器队列管理器
   */
  public MappingRuleValidationContextImpl(CapacitySchedulerQueueManager qm) {
    queueManager = qm;
  }

  /**
   * 验证静态队列路径是否合法（路径不包含任何可替换变量）
   * @param path 待验证的静态队列路径
   * @return 验证通过返回true
   * @throws YarnException 路径不合法时抛出异常
   */
  private boolean validateStaticQueuePath(QueuePath path)
      throws YarnException {
    // 规范化根路径格式
    String normalizedPath = MappingRuleValidationHelper.normalizeQueuePathRoot(
        queueManager, path.getFullPath());
    // 执行队列路径自动创建合法性检查
    MappingRuleValidationHelper.ValidationResult validity =
        MappingRuleValidationHelper.validateQueuePathAutoCreation(
            queueManager, normalizedPath);

    switch (validity) {
    case AMBIGUOUS_PARENT:
      throw new YarnException("Target queue path '" + path +
          "' contains an ambiguous parent queue '" +
          path.getParent() + "' reference.");
    case AMBIGUOUS_QUEUE:
      throw new YarnException("Target queue is an ambiguous leaf queue '" +
              path.getFullPath() + "'.");
    case EMPTY_PATH:
      throw new YarnException("Mapping rule did not specify a target queue.");
    case NO_PARENT_PROVIDED:
      throw new YarnException(
          "Target queue does not exist and has no parent defined '" +
              path.getFullPath() + "'.");
    case NO_DYNAMIC_PARENT:
      throw new YarnException("Mapping rule specified a parent queue '" +
          path.getParent() + "', but it is not a dynamic parent queue, " +
          "and no queue exists with name '" + path.getLeafName() +
          "' under it.");
    case QUEUE_EXISTS:
      // 队列已存在时，验证必须是叶子队列才能放置应用
      CSQueue queue = queueManager.getQueue(normalizedPath);
      if (!(queue instanceof AbstractLeafQueue)) {
        throw new YarnException("Target queue '" + path.getFullPath() +
            "' but it's not a leaf queue.");
      }
      break;
    case CREATABLE:
      // 队列不存在但支持自动创建，验证通过
      break;
    default:
      // 未知验证结果，抛出异常
      throw new YarnException("Unknown queue path validation result. '" +
          validity + "'.");
    }

    return true;
  }

  /**
   * 验证包含变量的动态队列路径是否合法
   * @param path 待验证的动态队列路径
   * @return 验证通过返回true
   * @throws YarnException 路径不合法时抛出异常
   */
  private boolean validateDynamicQueuePath(QueuePath path)
      throws YarnException{
    // 按点分割路径为多个片段
    ArrayList<String> parts = new ArrayList<>();
    Collections.addAll(parts, path.getFullPath().split("\\."));

    Iterator<String> pointer = parts.iterator();
    if (!pointer.hasNext()) {
      // 空路径异常（理论上不会触发）
      throw new YarnException("Empty queue path provided '" + path + "'");
    }
    // 初始化静态部分缓冲区，取第一个路径片段
    StringBuilder staticPartBuffer = new StringBuilder(pointer.next());
    String staticPartParent = null;

    // 如果根路径本身就是动态的，无法进一步验证，直接通过
    if (!isPathStatic(staticPartBuffer.toString())) {
      return true;
    }

    // 遍历收集前缀所有静态片段，直到遇到第一个动态片段停止
    while (pointer.hasNext()) {
      String nextPart = pointer.next();
      if (isPathStatic(nextPart)) {
        staticPartParent = staticPartBuffer.toString();
        staticPartBuffer.append(DOT).append(nextPart);
      } else {
        // 找到第一个动态片段，停止遍历
        break;
      }
    }
    String staticPart = staticPartBuffer.toString();

    // 规范化静态前缀路径
    String normalizedStaticPart =
        MappingRuleValidationHelper.normalizeQueuePathRoot(
            queueManager, staticPart);
    CSQueue queue = queueManager.getQueue(normalizedStaticPart);
    // 静态前缀已存在的情况
    if (queue != null) {
      // 如果静态前缀本身是叶子队列，无法再创建子队列，验证失败
      if (queue instanceof AbstractLeafQueue) {
        throw new YarnException("Queue path '" + path +"' is invalid " +
            "because '" + normalizedStaticPart + "' is a leaf queue, " +
            "which can have no other queues under it.");
      }
      // 静态前缀是父队列，验证通过
      return true;
    }

    // 静态前缀不存在，检查其父节点是否支持动态创建
    if (staticPartParent != null) {
      String normalizedStaticPartParent
          = MappingRuleValidationHelper.normalizeQueuePathRoot(
              queueManager, staticPartParent);
      queue = queueManager.getQueue(normalizedStaticPartParent);
      // 父节点支持动态创建子队列，验证通过
      if (isDynamicParent(queue)) {
        return true;
      }
    }

    // 找不到符合要求的父节点支持动态创建，验证失败
    throw new YarnException("No eligible parent found on path '" + path + "'.");
  }

  /**
   * 判断队列是否支持作为动态父队列（可以自动创建子队列）
   * @param queue 待判断的队列对象
   * @return 支持自动创建子队列返回true，否则返回false
   */
  private boolean isDynamicParent(CSQueue queue) {
    if (queue == null) {
      return false;
    }

    if (queue instanceof ManagedParentQueue) {
      return true;
    }

    if (queue instanceof ParentQueue) {
      return ((ParentQueue)queue).isEligibleForAutoQueueCreation();
    }

    return false;
  }


  /**
   * 对外暴露的队列路径验证入口方法，区分静态和动态路径分别验证
   * @param queuePath 待验证的队列路径
   * @return 验证通过返回true
   * @throws YarnException 路径不合法时抛出异常
   */
  public boolean validateQueuePath(String queuePath) throws YarnException {
    if (queuePath == null || queuePath.isEmpty()) {
      throw new YarnException("Queue path is empty.");
    }
    QueuePath path = new QueuePath(queuePath);

    if (isPathStatic(queuePath)) {
      return validateStaticQueuePath(path);
    } else {
      return validateDynamicQueuePath(path);
    }
  }

  /**
   * 判断整个队列路径是否是静态（不包含任何动态变量片段）
   * @param queuePath 待检查的队列路径
   * @return 无动态片段返回true
   * @throws YarnException 路径包含空片段时抛出异常
   */
  public boolean isPathStatic(String queuePath) throws YarnException {
    String[] parts = queuePath.split("\\.");
    for (int i = 0; i < parts.length; i++) {
      if (parts[i].isEmpty()) {
        throw new YarnException("Path segment cannot be empty '" +
            queuePath + "'.");
      }

      if (!isPathPartStatic(parts[i])) {
        return false;
      }
    }

    return true;
  }

  /**
   * 判断单个路径片段是否是静态（不匹配已知变量）
   * @param pathPart 待检查的路径片段
   * @return 不是动态变量返回true
   */
  private boolean isPathPartStatic(String pathPart) {
    if (knownVariables.contains(pathPart)) {
      return false;
    }

    return true;
  }

  /**
   * 添加上下文已知的可变变量，用于判断路径是否为动态
   * @param variable 变量名称
   * @throws YarnException 变量已被标记为不可变时抛出异常
   */
  public void addVariable(String variable) throws YarnException {
    if (immutableVariables.contains(variable)) {
      throw new YarnException("Variable '" + variable + "' is immutable " +
          "cannot add to the modified variable list.");
    }
    knownVariables.add(variable);
  }

  /**
   * 添加上下文已知的不可变变量，用于判断路径是否为动态
   * @param variable 不可变变量名称
   * @throws YarnException 变量已作为可变变量添加时抛出异常
   */
  public void addImmutableVariable(String variable) throws YarnException {
    if (knownVariables.contains(variable) &&
        !immutableVariables.contains(variable)) {
      throw new YarnException("Variable '" + variable + "' already " +
          "added as a mutable variable cannot set it to immutable.");
    }
    knownVariables.add(variable);
    immutableVariables.add(variable);
  }

  /**
   * 获取所有已知变量的不可变拷贝
   * @return 所有已知变量的不可变集合
   */
  public Set<String> getVariables() {
    return ImmutableSet.copyOf(knownVariables);
  }
}