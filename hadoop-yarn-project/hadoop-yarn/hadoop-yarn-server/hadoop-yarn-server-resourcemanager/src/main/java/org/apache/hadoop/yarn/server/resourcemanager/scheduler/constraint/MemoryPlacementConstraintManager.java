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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
管理员
org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN调度 placement约束管理器的内存实现，基于内存存储全局和应用级的放置约束规则。
 * 继承自PlacementConstraintManagerService抽象服务，提供线程安全的约束增删查改能力。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MemoryPlacementConstraintManager
    extends PlacementConstraintManagerService {

  private static final Logger LOG =
      LoggerFactory.getLogger(MemoryPlacementConstraintManager.class);

  private ReentrantReadWriteLock.ReadLock readLock;
  private ReentrantReadWriteLock.WriteLock writeLock;

  /**
   * 存储集群管理员配置的全局放置约束，key为触发约束的标签，value为对应约束规则。
   */
  private Map<String, PlacementConstraint> globalConstraints;
  /**
   * 存储每个应用的放置约束，外层key为应用ID，内层map结构同全局约束：key为触发约束的标签，value为约束规则。
   */
  private Map<ApplicationId, Map<String, PlacementConstraint>> appConstraints;

  /**
   * 构造内存版约束管理器，初始化存储结构和读写锁。
   */
  public MemoryPlacementConstraintManager() {
    this.globalConstraints = new HashMap<>();
    this.appConstraints = new HashMap<>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 调用父类服务初始化逻辑
    super.serviceInit(conf);
  }

  @Override
  /**
   * 注册应用，将应用的所有初始约束存入管理器。
   * @param appId 应用ID
   * @param constraintMap 应用初始约束映射表，key为触发约束的标签集合，value为约束规则
   */
  public void registerApplication(ApplicationId appId,
      Map<Set<String>, PlacementConstraint> constraintMap) {
    // Check if app already exists. If not, prepare its constraint map.
    Map<String, PlacementConstraint> constraintsForApp = new HashMap<>();
    readLock.lock();
    try {
      // 检查应用是否已注册
      if (appConstraints.get(appId) != null) {
        LOG.warn("Application {} has already been registered.", appId);
        return;
      }
      // 遍历所有约束对，验证后加入应用约束映射表
      for (Map.Entry<Set<String>, PlacementConstraint> entry : constraintMap
          .entrySet()) {
        Set<String> sourceTags = entry.getKey();
        PlacementConstraint constraint = entry.getValue();
        if (validateConstraint(sourceTags, constraint)) {
          String sourceTag = getValidSourceTag(sourceTags);
          constraintsForApp.put(sourceTag, constraint);
        }
      }
    } finally {
      readLock.unlock();
    }

    if (constraintsForApp.isEmpty()) {
      LOG.info("Application {} was registered, but no constraints were added.",
          appId);
    }
    // 更新全局应用约束表，加写锁保证线程安全
    writeLock.lock();
    try {
      appConstraints.put(appId, constraintsForApp);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  /**
   * 为已注册应用添加新的放置约束。
   * @param appId 目标应用ID
   * @param sourceTags 触发该约束的标签集合
   * @param placementConstraint 待添加的约束规则
   * @param replace 是否覆盖已存在的同名约束
   */
  public void addConstraint(ApplicationId appId, Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace) {
    writeLock.lock();
    try {
      Map<String, PlacementConstraint> constraintsForApp =
          appConstraints.get(appId);
      // 应用未注册时无法添加约束
      if (constraintsForApp == null) {
        LOG.info("Cannot add constraint to application {}, as it has not "
            + "been registered yet.", appId);
        return;
      }

      addConstraintToMap(constraintsForApp, sourceTags, placementConstraint,
          replace);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  /**
   * 添加全局放置约束，对所有应用生效。
   * @param sourceTags 触发该约束的标签集合
   * @param placementConstraint 待添加的全局约束规则
   * @param replace 是否覆盖已存在的同名约束
   */
  public void addGlobalConstraint(Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace) {
    writeLock.lock();
    try {
      addConstraintToMap(globalConstraints, sourceTags, placementConstraint,
          replace);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 辅助方法：将约束添加到指定约束映射表，处理验证、覆盖逻辑。
   * 调用方需要保证已获取对应写锁。
   *
   * @param constraintMap 目标约束映射表
   * @param sourceTags 触发约束的标签集合
   * @param placementConstraint 待添加的约束
   * @param replace 是否覆盖已有约束
   */
  private void addConstraintToMap(
      Map<String, PlacementConstraint> constraintMap, Set<String> sourceTags,
      PlacementConstraint placementConstraint, boolean replace) {
    if (validateConstraint(sourceTags, placementConstraint)) {
      String sourceTag = getValidSourceTag(sourceTags);
      if (constraintMap.get(sourceTag) == null || replace) {
        if (replace) {
          LOG.info("Replacing the constraint associated with tag {} with {}.",
              sourceTag, placementConstraint);
        }
        constraintMap.put(sourceTag, placementConstraint);
      } else {
        LOG.info("Constraint {} will not be added. There is already a "
                + "constraint associated with tag {}.",
            placementConstraint, sourceTag);
      }
    }
  }

  @Override
  /**
   * 获取指定应用的所有放置约束，返回不可修改映射表保证线程安全。
   * @param appId 目标应用ID
   * @return 应用所有约束的不可修改映射表，应用未注册返回null
   */
  public Map<Set<String>, PlacementConstraint> getConstraints(
      ApplicationId appId) {
    readLock.lock();
    try {
      if (appConstraints.get(appId) == null) {
        LOG.debug("Application {} is not registered in the Placement "
            + "Constraint Manager.", appId);
        return null;
      }

      // 转换格式为key为Set<String>的映射表，返回不可修改版本
      Map<Set<String>, PlacementConstraint> constraintMap =
          appConstraints.get(appId).entrySet().stream()
              .collect(Collectors.toMap(
                  e -> Stream.of(e.getKey()).collect(Collectors.toSet()),
                  e -> e.getValue()));

      return Collections.unmodifiableMap(constraintMap);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  /**
   * 获取指定应用指定标签对应的放置约束。
   * @param appId 目标应用ID
   * @param sourceTags 触发约束的标签集合
   * @return 匹配的约束规则，不存在或验证失败返回null
   */
  public PlacementConstraint getConstraint(ApplicationId appId,
      Set<String> sourceTags) {
    if (!validateSourceTags(sourceTags)) {
      return null;
    }
    String sourceTag = getValidSourceTag(sourceTags);
    readLock.lock();
    try {
      if (appConstraints.get(appId) == null) {
        LOG.debug("Application {} is not registered in the Placement "
            + "Constraint Manager.", appId);
        return null;
      }
      // TODO: Merge this constraint with the global one for this tag, if one
      // exists.
      return appConstraints.get(appId).get(sourceTag);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  /**
   * 获取指定标签对应的全局放置约束。
   * @param sourceTags 触发约束的标签集合
   * @return 匹配的全局约束规则，不存在或验证失败返回null
   */
  public PlacementConstraint getGlobalConstraint(Set<String> sourceTags) {
    if (!validateSourceTags(sourceTags)) {
      return null;
    }
    String sourceTag = getValidSourceTag(sourceTags);
    readLock.lock();
    try {
      return globalConstraints.get(sourceTag);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  /**
   * 合并请求级、应用级、全局三级约束为一个AND组合约束，要求所有约束都必须满足。
   * 合并顺序为 CC = AND(全局约束, 应用级约束, 请求级约束)，保证所有层级约束都生效。
   * @param appId 目标应用ID
   * @param sourceTags 触发约束的标签集合
   * @param schedulingRequestConstraint 请求级约束
   * @return 合并后的AND组合约束
   */
  public PlacementConstraint getMultilevelConstraint(ApplicationId appId,
      Set<String> sourceTags, PlacementConstraint schedulingRequestConstraint) {
    List<PlacementConstraint> constraints = new ArrayList<>();
    // 添加请求级约束
    if (schedulingRequestConstraint != null) {
      constraints.add(schedulingRequestConstraint);
    }
    // 添加应用级约束（应用存在且标签非空时）
    if (appId != null && sourceTags != null
        && !sourceTags.isEmpty()) {
      constraints.add(getConstraint(appId, sourceTags));
    }
    // 添加全局约束（标签非空时）
    if (sourceTags != null && !sourceTags.isEmpty()) {
      constraints.add(getGlobalConstraint(sourceTags));
    }

    // 过滤空值和重复约束，提取约束表达式去重
    List<PlacementConstraint.AbstractConstraint> allConstraints =
        constraints.stream()
            .filter(placementConstraint -> placementConstraint != null
            && placementConstraint.getConstraintExpr() != null)
            .map(PlacementConstraint::getConstraintExpr)
            .distinct()
            .collect(Collectors.toList());

    // 构造AND组合约束并返回
    PlacementConstraint.And andConstraint = PlacementConstraints.and(
        allConstraints.toArray(new PlacementConstraint
            .AbstractConstraint[allConstraints.size()]));
    return andConstraint.build();
  }

  @Override
  /**
   * 注销应用，从管理器中移除该应用的所有约束。
   * @param appId 待注销的应用ID
   */
  public void unregisterApplication(ApplicationId appId) {
    writeLock.lock();
    try {
      appConstraints.remove(appId);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  /**
   * 移除指定标签对应的全局约束。
   * @param sourceTags 目标标签集合
   */
  public void removeGlobalConstraint(Set<String> sourceTags) {
    if (!validateSourceTags(sourceTags)) {
      return;
    }
    String sourceTag = getValidSourceTag(sourceTags);
    writeLock.lock();
    try {
      globalConstraints.remove(sourceTag);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  /**
   * 获取当前已注册应用数量。
   * @return 已注册应用总数
   */
  public int getNumRegisteredApplications() {
    readLock.lock();
    try {
      return appConstraints.size();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  /**
   * 获取当前全局约束数量。
   * @return 全局约束总数
   */
  public int getNumGlobalConstraints() {
    readLock.lock();
    try {
      return globalConstraints.size();
    } finally {
      readLock.unlock();
    }
  }
}