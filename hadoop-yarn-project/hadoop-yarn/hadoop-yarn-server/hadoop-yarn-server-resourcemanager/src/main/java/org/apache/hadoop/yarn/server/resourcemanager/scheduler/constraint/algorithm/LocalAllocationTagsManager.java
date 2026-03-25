// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTags;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongBinaryOperator;

/**
 * 本地分配标签管理器，负责在单次调度 placement 周期内维护临时分配标签，
 * 用于放置算法计算过程中跟踪待分配容器的标签计数，最终在放置周期结束后清理临时数据
 */
class LocalAllocationTagsManager extends AllocationTagsManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalAllocationTagsManager.class);

  /** 全局全局标签管理器引用，负责持久化维护已分配容器标签计数 */
  private final AllocationTagsManager tagsManager;

  /** 应用 -> 节点 -> 标签 -> 临时计数 三级映射，存储单次放置周期内的临时标签计数 */
  // Application's Temporary containers mapping
  private Map<ApplicationId, Map<NodeId, Map<String, AtomicInteger>>>
      appTempMappings = new HashMap<>();

  /**
   * 构造方法，依赖全局标签管理器
   * @param allocationTagsManager 全局标签管理器实例
   */
  LocalAllocationTagsManager(
      AllocationTagsManager allocationTagsManager) {
    super(null);
    this.tagsManager = allocationTagsManager;
  }

  /**
   * 添加临时分配标签，更新本地临时计数并同步到全局标签管理器
   * @param nodeId 节点ID
   * @param applicationId 应用ID
   * @param allocationTags 需要添加的分配标签集合
   */
  void addTempTags(NodeId nodeId,
      ApplicationId applicationId, Set<String> allocationTags) {
    // 获取或创建应用对应的临时映射
    Map<NodeId, Map<String, AtomicInteger>> appTempMapping =
        appTempMappings.computeIfAbsent(applicationId, k -> new HashMap<>());
    // 获取或创建节点对应的临时标签映射
    Map<String, AtomicInteger> containerTempMapping =
        appTempMapping.computeIfAbsent(nodeId, k -> new HashMap<>());
    // 遍历标签，递增临时计数
    for (String tag : allocationTags) {
      containerTempMapping.computeIfAbsent(tag,
          k -> new AtomicInteger(0)).incrementAndGet();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added TEMP container with tags=["
          + StringUtils.join(allocationTags, ",") + "]");
    }
    // 同步添加到全局标签管理器，用于Placement阶段的约束计算
    tagsManager.addTags(nodeId, applicationId, allocationTags);
  }

  /**
   * 移除临时分配标签，递减本地临时计数并从全局标签管理器移除
   * @param nodeId 节点ID
   * @param applicationId 应用ID
   * @param allocationTags 需要移除的分配标签集合
   */
  void removeTempTags(NodeId nodeId, ApplicationId applicationId,
      Set<String> allocationTags) {
    // 获取应用对应的临时映射
    Map<NodeId, Map<String, AtomicInteger>> appTempMapping =
        appTempMappings.get(applicationId);
    if (appTempMapping != null) {
      // 获取节点对应的临时标签映射
      Map<String, AtomicInteger> containerTempMap =
          appTempMapping.get(nodeId);
      if (containerTempMap != null) {
        // 遍历标签，递减计数，计数归零后移除标签
        for (String tag : allocationTags) {
          AtomicInteger count = containerTempMap.get(tag);
          if (count != null) {
            if (count.decrementAndGet() <= 0) {
              containerTempMap.remove(tag);
            }
          }
        }
      }
    }
    if (allocationTags != null) {
      removeTags(nodeId, applicationId, allocationTags);
    }
  }

  /**
   * 清理应用本次放置周期产生的所有临时标签，在放置周期结束后调用
   * @param applicationId Application Id.
   */
  public void cleanTempContainers(ApplicationId applicationId) {

    if (!appTempMappings.get(applicationId).isEmpty()) {
      // 遍历节点和标签，逐个从全局管理器移除对应计数的临时标签
      appTempMappings.get(applicationId).entrySet().stream().forEach(nodeE -> {
        nodeE.getValue().entrySet().stream().forEach(tagE -> {
          for (int i = 0; i < tagE.getValue().get(); i++) {
            removeTags(nodeE.getKey(), applicationId,
                Collections.singleton(tagE.getKey()));
          }
        });
      });
      // 移除本地临时映射
      appTempMappings.remove(applicationId);
      LOG.debug("Removed TEMP containers of app={}", applicationId);
    }
  }

  @Override
  public void addContainer(NodeId nodeId, ContainerId containerId,
      Set<String> allocationTags) {
    tagsManager.addContainer(nodeId, containerId, allocationTags);
  }

  @Override
  public void removeContainer(NodeId nodeId, ContainerId containerId,
      Set<String> allocationTags) {
    tagsManager.removeContainer(nodeId, containerId, allocationTags);
  }

  @Override
  public void removeTags(NodeId nodeId, ApplicationId applicationId,
      Set<String> allocationTags) {
    tagsManager.removeTags(nodeId, applicationId, allocationTags);
  }

  @Override
  public long getNodeCardinality(NodeId nodeId, ApplicationId applicationId,
      String tag) throws InvalidAllocationTagsQueryException {
    return tagsManager.getNodeCardinality(nodeId, applicationId, tag);
  }

  @Override
  public long getNodeCardinalityByOp(NodeId nodeId, AllocationTags tags,
      LongBinaryOperator op) throws InvalidAllocationTagsQueryException {
    return tagsManager.getNodeCardinalityByOp(nodeId, tags, op);
  }

  @Override
  public long getRackCardinality(String rack, ApplicationId applicationId,
      String tag) throws InvalidAllocationTagsQueryException {
    return tagsManager.getRackCardinality(rack, applicationId, tag);
  }

  @Override
  public long getRackCardinalityByOp(String rack, AllocationTags tags,
      LongBinaryOperator op) throws InvalidAllocationTagsQueryException {
    return tagsManager.getRackCardinalityByOp(rack, tags, op);
  }

  @Override
  public boolean allocationTagExistsOnNode(NodeId nodeId,
      ApplicationId applicationId, String tag)
      throws InvalidAllocationTagsQueryException {
    return tagsManager.allocationTagExistsOnNode(nodeId, applicationId, tag);
  }
}