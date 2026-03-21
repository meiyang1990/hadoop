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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.iterators.PopularTagsIterator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.iterators.SerialIterator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmInput;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 批量调度请求容器，将多个调度请求分组后批量提交给放置算法处理。
 * 批量处理可以让放置算法得到更优化的整体放置结果。
 */
public class BatchedRequests
    implements ConstraintPlacementAlgorithmInput, Iterable<SchedulingRequest> {

  // 当前批次已被调度器拒绝的放置尝试次数
  private final int placementAttempt;

  private final ApplicationId applicationId;
  private final Collection<SchedulingRequest> requests;
  // 按标签分组的黑名单节点，放置算法不会将容器分配到这些节点
  private final Map<String, Set<NodeId>> blacklist = new HashMap<>();
  // 请求遍历迭代器类型，决定批量请求的遍历顺序
  private IteratorType iteratorType;

  /**
   * 迭代器类型枚举，定义不同的批量请求遍历策略。
   */
  public enum IteratorType {
    /** 串行顺序遍历 */
    SERIAL,
    /** 按标签热度优先遍历 */
    POPULAR_TAGS
  }

  /**
   * 构造批量请求容器。
   * @param type 迭代器类型，指定遍历策略
   * @param applicationId 所属应用ID
   * @param requests 批量调度请求集合
   * @param attempt 当前放置尝试次数
   */
  public BatchedRequests(IteratorType type, ApplicationId applicationId,
      Collection<SchedulingRequest> requests, int attempt) {
    this.iteratorType = type;
    this.applicationId = applicationId;
    this.requests = requests;
    this.placementAttempt = attempt;
  }

  /**
   * 根据配置的迭代器类型，返回对应策略的调度请求迭代器。
   * @return 对应策略的调度请求迭代器
   */
  @Override
  public Iterator<SchedulingRequest> iterator() {
    switch (this.iteratorType) {
    case SERIAL:
      return new SerialIterator(requests);
    case POPULAR_TAGS:
      return new PopularTagsIterator(requests);
    default:
      return null;
    }
  }

  /**
   * 获取所属应用ID。
   * @return 应用ID
   */
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  /**
   * 获取当前批次包含的所有调度请求。
   * @return 调度请求集合
   */
  @Override
  public Collection<SchedulingRequest> getSchedulingRequests() {
    return requests;
  }

  /**
   * 添加单个调度请求到当前批次。
   * @param req 待添加的调度请求
   */
  public void addToBatch(SchedulingRequest req) {
    requests.add(req);
  }

  /**
   * 将节点添加到对应标签的黑名单中，禁止该标签的请求分配到该节点。
   * 当前仅支持每个调度请求单个分配标签。
   * @param tags 关联标签集合
   * @param node 待拉黑节点
   */
  public void addToBlacklist(Set<String> tags, SchedulerNode node) {
    if (tags != null && !tags.isEmpty() && node != null) {
      // 目前假设每个调度请求只有一个分配标签
      blacklist.computeIfAbsent(tags.iterator().next(),
          k -> new HashSet<>()).add(node.getNodeID());
    }
  }

  /**
   * 获取当前批次的放置尝试次数。
   * @return 放置尝试次数
   */
  public int getPlacementAttempt() {
    return placementAttempt;
  }

  /**
   * 获取指定标签对应的所有黑名单节点。
   * @param tag 目标标签
   * @return 该标签的黑名单节点集合，无则返回空集合
   */
  public Set<NodeId> getBlacklist(String tag) {
    return blacklist.getOrDefault(tag, Collections.emptySet());
  }

  /**
   * 获取当前配置的迭代器类型。
   * @return 迭代器类型
   */
  public IteratorType getIteratorType() {
    return iteratorType;
  }
}