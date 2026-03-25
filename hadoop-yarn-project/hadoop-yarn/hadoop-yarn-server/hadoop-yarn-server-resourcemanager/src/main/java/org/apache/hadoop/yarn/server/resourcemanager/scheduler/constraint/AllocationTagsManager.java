// 这个文件已经全部加上中文注释
/*
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
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongBinaryOperator;

/**
 * 分配标签管理器，维护应用/容器标签与节点/机架的内存映射关系，为亲和性/反亲和性放置与基数调度约束提供数据支持
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AllocationTagsManager {

  private static final Logger LOG = Logger.getLogger(
      AllocationTagsManager.class);

  // 读写锁读锁，保护并发访问映射数据
  private ReentrantReadWriteLock.ReadLock readLock;
  // 读写锁写锁，保护并发访问映射数据
  private ReentrantReadWriteLock.WriteLock writeLock;
  // YARN RM上下文对象
  private final RMContext rmContext;

  // 应用标签到节点的映射，每个应用单独维护
  private Map<ApplicationId, TypeToCountedTags> perAppNodeMappings =
      new HashMap<>();
  // 应用标签到机架的映射，每个应用单独维护
  private Map<ApplicationId, TypeToCountedTags> perAppRackMappings =
      new HashMap<>();

  // 全局标签到节点的映射，用于快速跨应用聚合标签基数统计
  private TypeToCountedTags<NodeId> globalNodeMapping = new TypeToCountedTags();
  // 全局标签到机架的映射
  private TypeToCountedTags<String> globalRackMapping = new TypeToCountedTags();

  /**
   * 泛型存储结构，将类型T映射到带计数的标签集合，支持节点和机架两种类型的存储
   * 内部结构为: Map<类型T, Map<标签名称, 计数>>
   */
  @VisibleForTesting
  public static class TypeToCountedTags<T> {
    // 存储类型到标签计数的双层映射
    private Map<T, Map<String, Long>> typeToTagsWithCount = new HashMap<>();

    public TypeToCountedTags() {}

    private TypeToCountedTags(Map<T, Map<String, Long>> tags) {
      this.typeToTagsWithCount = tags;
    }

    // 由外部锁保护，批量添加标签并递增计数
    private void addTags(T type, Set<String> tags) {
      Map<String, Long> innerMap =
          typeToTagsWithCount.computeIfAbsent(type, k -> new HashMap<>());

      for (String tag : tags) {
        Long count = innerMap.get(tag);
        if (count == null) {
          innerMap.put(tag, 1L);
        } else {
          innerMap.put(tag, count + 1);
        }
      }
    }

    // 由外部锁保护，添加单个标签并递增计数
    private void addTag(T type, String tag) {
      Map<String, Long> innerMap =
          typeToTagsWithCount.computeIfAbsent(type, k -> new HashMap<>());

      Long count = innerMap.get(tag);
      if (count == null) {
        innerMap.put(tag, 1L);
      } else {
        innerMap.put(tag, count + 1);
      }
    }

    // 从内部映射中移除单个标签，处理计数递减和空清理
    private void removeTagFromInnerMap(Map<String, Long> innerMap, String tag) {
      Long count = innerMap.get(tag);
      if (count == null) {
        LOG.warn("Trying to remove tags, however the tag " + tag
            + " no longer exists on this node/rack.");
        return;
      }
      if (count > 1) {
        innerMap.put(tag, count - 1);
      } else {
        if (count <= 0) {
          LOG.warn(
              "Trying to remove tags from node/rack, however the count already"
                  + " becomes 0 or less, it could be a potential bug.");
        }
        innerMap.remove(tag);
      }
    }

    // 批量移除标签并递减计数
    private void removeTags(T type, Set<String> tags) {
      Map<String, Long> innerMap = typeToTagsWithCount.get(type);
      if (innerMap == null) {
        LOG.warn("Failed to find node/rack=" + type
            + " while trying to remove tags, please double check.");
        return;
      }

      for (String tag : tags) {
        removeTagFromInnerMap(innerMap, tag);
      }

      if (innerMap.isEmpty()) {
        typeToTagsWithCount.remove(type);
      }
    }

    // 移除单个标签并递减计数
    private void removeTag(T type, String tag) {
      Map<String, Long> innerMap = typeToTagsWithCount.get(type);
      if (innerMap == null) {
        LOG.warn("Failed to find node/rack=" + type
            + " while trying to remove tags, please double check.");
        return;
      }

      removeTagFromInnerMap(innerMap, tag);

      if (innerMap.isEmpty()) {
        typeToTagsWithCount.remove(type);
      }
    }

    // 获取指定类型上单个标签的基数（分配数量）
    private long getCardinality(T type, String tag) {
      Map<String, Long> innerMap = typeToTagsWithCount.get(type);
      if (innerMap == null) {
        return 0;
      }
      Long value = innerMap.get(tag);
      return value == null ? 0 : value;
    }

    // 使用自定义二元操作符，计算多个标签在指定类型上的聚合基数
    private long getCardinality(T type, Set<String> tags,
        LongBinaryOperator op) {
      Map<String, Long> innerMap = typeToTagsWithCount.get(type);
      if (innerMap == null) {
        return 0;
      }

      long returnValue = 0;
      boolean firstTag = true;

      if (tags != null && !tags.isEmpty()) {
        for (String tag : tags) {
          Long value = innerMap.get(tag);
          if (value == null) {
            value = 0L;
          }

          if (firstTag) {
            returnValue = value;
            firstTag = false;
            continue;
          }

          returnValue = op.applyAsLong(returnValue, value);
        }
      } else {
        // 未指定标签时，遍历当前类型所有标签进行聚合，性能更优
        for (long value : innerMap.values()) {
          // 第一个值不需要应用操作符
          if (firstTag) {
            returnValue = value;
            firstTag = false;
            continue;
          }
          returnValue = op.applyAsLong(returnValue, value);
        }
      }
      return returnValue;
    }

    // 检查当前存储是否为空
    private boolean isEmpty() {
      return typeToTagsWithCount.isEmpty();
    }

    @VisibleForTesting
    public Map<T, Map<String, Long>> getTypeToTagsWithCount() {
      return typeToTagsWithCount;
    }

    /**
     * 吸收合并另一个TypeToCountedTags对象到当前映射，相同标签的计数会累加
     * @param target 待合并的目标对象
     */
    protected void absorb(final TypeToCountedTags<T> target) {
      // 目标为空时不做处理
      if (target == null || target.getTypeToTagsWithCount() == null) {
        return;
      }

      // 遍历目标映射进行合并
      Map<T, Map<String, Long>> targetMap = target.getTypeToTagsWithCount();
      for (Map.Entry<T, Map<String, Long>> targetEntry :
          targetMap.entrySet()) {
        // 创建可变拷贝，不修改原目标对象引用
        Map<String, Long> copy = Maps.newHashMap(targetEntry.getValue());

        // 当前不存在该类型，直接添加新条目
        Map<String, Long> existingMapping =
            this.typeToTagsWithCount.putIfAbsent(targetEntry.getKey(), copy);
        // 当前已存在该类型，逐标签合并计数
        if (existingMapping != null) {
          Map<String, Long> localMap =
              this.typeToTagsWithCount.get(targetEntry.getKey());
          // 将目标标签计数合并到当前内部映射
          Map<String, Long> targetValue = targetEntry.getValue();
          for (Map.Entry<String, Long> entry : targetValue.entrySet()) {
            localMap.merge(entry.getKey(), entry.getValue(),
                (a, b) -> Long.sum(a, b));
          }
        }
      }
    }

    /**
     * 创建当前实例的不可变拷贝
     * @return 不可变拷贝对象
     */
    protected TypeToCountedTags immutableCopy() {
      return new TypeToCountedTags(
          Collections.unmodifiableMap(this.typeToTagsWithCount));
    }
  }

  @VisibleForTesting
  public Map<ApplicationId, TypeToCountedTags> getPerAppNodeMappings() {
    return perAppNodeMappings;
  }

  @VisibleForTesting
  Map<ApplicationId, TypeToCountedTags> getPerAppRackMappings() {
    return perAppRackMappings;
  }

  @VisibleForTesting
  TypeToCountedTags getGlobalNodeMapping() {
    return globalNodeMapping;
  }

  @VisibleForTesting
  TypeToCountedTags getGlobalRackMapping() {
    return globalRackMapping;
  }

  /**
   * 构造分配标签管理器
   * @param context RM上下文对象
   */
  public AllocationTagsManager(RMContext context) {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    rmContext = context;
  }

  /**
   * 根据分配标签定义的命名空间范围，聚合多个应用的标签映射为单个合并后的对象
   * @param allocationTags 分配标签对象，包含命名空间范围定义
   * @param mapping 待聚合的映射表（节点或机架）
   * @return 聚合后的标签映射对象
   * @throws InvalidAllocationTagsQueryException 非法查询参数时抛出
   */
  private TypeToCountedTags aggregateAllocationTags(
      AllocationTags allocationTags,
      Map<ApplicationId, TypeToCountedTags> mapping)
      throws InvalidAllocationTagsQueryException {
    // 根据分配标签的命名空间类型解析范围
    TargetApplicationsNamespace namespace = allocationTags.getNamespace();
    TargetApplications ta = new TargetApplications(
        allocationTags.getCurrentApplicationId(), getApplicationIdToTags());
    namespace.evaluate(ta);
    Set<ApplicationId> appIds = namespace.getNamespaceScope();
    TypeToCountedTags result = new TypeToCountedTags();
    if (appIds != null) {
      // 仅单个应用时直接返回原映射，无需额外计算
      if (appIds.size() == 1) {
        return mapping.get(appIds.iterator().next());
      }

      // 遍历范围包含的所有应用，合并标签计数
      for (ApplicationId applicationId : appIds) {
        TypeToCountedTags appIdTags = mapping.get(applicationId);
        if (appIdTags != null) {
          // 合并不可变拷贝，保证不会修改原状态
          result.absorb(appIdTags.immutableCopy());
        }
      }
    }
    return result;
  }

  /**
   * 通知容器已分配到节点，添加对应分配标签
   * @param nodeId 分配的节点ID
   * @param containerId 容器ID
   * @param allocationTags 容器携带的分配标签集合
   */
  @SuppressWarnings("unchecked")
  public void addContainer(NodeId nodeId, ContainerId containerId,
      Set<String> allocationTags) {
    // 空标签不处理
    if (allocationTags == null || allocationTags.isEmpty()) {
      return;
    }
    ApplicationId applicationId =
        containerId.getApplicationAttemptId().getApplicationId();
    addTags(nodeId, applicationId, allocationTags);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added container=" + containerId + " with tags=["
          + StringUtils.join(allocationTags, ",") + "]");
    }
  }

  /**
   * 添加标签到应用和全局映射
   * @param nodeId 节点ID
   * @param applicationId 应用ID
   * @param allocationTags 待添加的标签集合
   */
  public void addTags(NodeId nodeId, ApplicationId applicationId,
      Set<String> allocationTags) {
    writeLock.lock();
    try {
      // 获取或创建应用节点标签映射
      TypeToCountedTags perAppTagsMapping = perAppNodeMappings
          .computeIfAbsent(applicationId, k -> new TypeToCountedTags());
      // 获取或创建应用机架标签映射
      TypeToCountedTags perAppRackTagsMapping = perAppRackMappings
          .computeIfAbsent(applicationId, k -> new TypeToCountedTags());
      // 获取节点所在机架名称，兼容测试mock场景
      String nodeRack = (rmContext.getRMNodes() != null
          && rmContext.getRMNodes().get(nodeId) != null)
              ? rmContext.getRMNodes().get(nodeId).getRackName() :
          "default-rack";
      // 更新应用级节点标签
      perAppTagsMapping.addTags(nodeId, allocationTags);
      // 更新应用级机架标签
      perAppRackTagsMapping.addTags(nodeRack, allocationTags);
      // 更新全局节点标签
      globalNodeMapping.addTags(nodeId, allocationTags);
      // 更新全局机架标签
      globalRackMapping.addTags(nodeRack, allocationTags);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 通知容器已移除，删除对应分配标签
   * @param nodeId 容器所在节点ID
   * @param containerId 容器ID
   * @param allocationTags 容器携带的分配标签集合
   */
  @SuppressWarnings("unchecked")
  public void removeContainer(NodeId nodeId,
      ContainerId containerId, Set<String> allocationTags) {
    // 空标签不处理
    if (allocationTags == null || allocationTags.isEmpty()) {
      return;
    }
    ApplicationId applicationId =
        containerId.getApplicationAttemptId().getApplicationId();

    removeTags(nodeId, applicationId, allocationTags);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removed container=" + containerId + " with tags=["
          + StringUtils.join(allocationTags, ",") + "]");
    }
  }

  /**
   * 从应用和全局映射中移除标签
   * @param nodeId 节点ID
   * @param applicationId 应用ID
   * @param allocationTags 待移除的标签集合
   */
  public void removeTags(NodeId nodeId, ApplicationId applicationId,
      Set<String> allocationTags) {
    writeLock.lock();
    try {
      // 获取应用对应映射
      TypeToCountedTags perAppTagsMapping =
          perAppNodeMappings.get(applicationId);
      TypeToCountedTags perAppRackTagsMapping =
          perAppRackMappings.get(applicationId);
      if (perAppTagsMapping == null) {
        return;
      }
      // 获取节点所在机架名称，兼容测试mock场景
      String nodeRack = (rmContext.getRMNodes() != null
          && rmContext.getRMNodes().get(nodeId) != null)
              ? rmContext.getRMNodes().get(nodeId).getRackName() :
          "default-rack";
      // 移除应用级节点标签计数
      perAppTagsMapping.removeTags(nodeId, allocationTags);
      // 移除应用级机架标签计数
      perAppRackTagsMapping.removeTags(nodeRack, allocationTags);
      // 移除全局