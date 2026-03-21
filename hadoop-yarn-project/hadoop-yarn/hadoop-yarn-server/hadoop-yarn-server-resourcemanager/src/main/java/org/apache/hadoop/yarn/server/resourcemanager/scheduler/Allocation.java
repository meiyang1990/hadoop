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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;

/**
 * YARN资源调度分配结果容器，封装一次调度中所有容器分配、抢占、资源调整相关结果
 * 保存调度器为应用计算得出的最终资源分配结果，供后续处理使用
 */
public class Allocation {

  // 新分配的容器列表
  final List<Container> containers;
  // 严格抢占的容器ID集合（必须回收的容器）
  final Set<ContainerId> strictContainers;
  // 可替代抢占的容器ID集合（可灵活回收的容器）
  final Set<ContainerId> fungibleContainers;
  // 可抢占资源请求列表
  final List<ResourceRequest> fungibleResources;
  // NodeManager令牌列表，用于容器身份认证
  final List<NMToken> nmTokens;
  // 资源增加的容器列表（弹性资源调整场景）
  final List<Container> increasedContainers;
  // 资源减少的容器列表（弹性资源调整场景）
  final List<Container> decreasedContainers;
  // 优先级提升的容器列表
  final List<Container> promotedContainers;
  // 优先级降低的容器列表
  final List<Container> demotedContainers;
  // 上一次尝试运行保留的容器列表
  private final List<Container> previousAttemptContainers;
  // 应用允许的总资源上限
  private Resource resourceLimit;
  // 被拒绝的调度请求列表
  private List<RejectedSchedulingRequest> rejectedRequest;

  /**
   * 构造分配结果，基础构造函数
   * @param containers 新分配容器列表
   * @param resourceLimit 应用资源上限
   * @param strictContainers 严格抢占容器ID集合
   * @param fungibleContainers 可替代抢占容器ID集合
   * @param fungibleResources 可抢占资源请求列表
   */
  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources) {
    this(containers,  resourceLimit,strictContainers,  fungibleContainers,
      fungibleResources, null);
  }

  /**
   * 构造分配结果，支持NM令牌
   * @param containers 新分配容器列表
   * @param resourceLimit 应用资源上限
   * @param strictContainers 严格抢占容器ID集合
   * @param fungibleContainers 可替代抢占容器ID集合
   * @param fungibleResources 可抢占资源请求列表
   * @param nmTokens NodeManager认证令牌列表
   */
  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens) {
    this(containers, resourceLimit, strictContainers, fungibleContainers,
        fungibleResources, nmTokens, null, null, null, null, null, null);
  }

  /**
   * 构造分配结果，支持弹性资源调整
   * @param containers 新分配容器列表
   * @param resourceLimit 应用资源上限
   * @param strictContainers 严格抢占容器ID集合
   * @param fungibleContainers 可替代抢占容器ID集合
   * @param fungibleResources 可抢占资源请求列表
   * @param nmTokens NodeManager认证令牌列表
   * @param increasedContainers 资源增加的容器列表
   * @param decreasedContainer 资源减少的容器列表
   */
  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens,
      List<Container> increasedContainers, List<Container> decreasedContainer) {
    this(containers, resourceLimit, strictContainers, fungibleContainers,
        fungibleResources, nmTokens, increasedContainers, decreasedContainer,
        null, null, null, null);
  }

  /**
   * 完整构造函数，包含所有分配结果字段
   * @param containers 新分配容器列表
   * @param resourceLimit 应用资源上限
   * @param strictContainers 严格抢占容器ID集合
   * @param fungibleContainers 可替代抢占容器ID集合
   * @param fungibleResources 可抢占资源请求列表
   * @param nmTokens NodeManager认证令牌列表
   * @param increasedContainers 资源增加的容器列表
   * @param decreasedContainer 资源减少的容器列表
   * @param promotedContainers 优先级提升的容器列表
   * @param demotedContainer 优先级降低的容器列表
   * @param previousAttemptContainers 上一次尝试保留容器列表
   * @param rejectedRequest 被拒绝的调度请求列表
   */
  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens,
      List<Container> increasedContainers, List<Container> decreasedContainer,
      List<Container> promotedContainers, List<Container> demotedContainer,
      List<Container> previousAttemptContainers, List<RejectedSchedulingRequest>
      rejectedRequest) {
    this.containers = containers;
    this.resourceLimit = resourceLimit;
    this.strictContainers = strictContainers;
    this.fungibleContainers = fungibleContainers;
    this.fungibleResources = fungibleResources;
    this.nmTokens = nmTokens;
    this.increasedContainers = increasedContainers;
    this.decreasedContainers = decreasedContainer;
    this.promotedContainers = promotedContainers;
    this.demotedContainers = demotedContainer;
    this.previousAttemptContainers = previousAttemptContainers;
    this.rejectedRequest = rejectedRequest;
  }

  /**
   * 获取本次分配的新容器列表
   * @return 新分配容器列表
   */
  public List<Container> getContainers() {
    return containers;
  }

  /**
   * 获取应用允许的总资源上限
   * @return 资源上限对象
   */
  public Resource getResourceLimit() {
    return resourceLimit;
  }

  /**
   * 获取需要严格抢占的容器ID集合
   * @return 严格抢占容器ID集合
   */
  public Set<ContainerId> getStrictContainerPreemptions() {
    return strictContainers;
  }

  /**
   * 获取可灵活抢占的容器ID集合
   * @return 可替代抢占容器ID集合
   */
  public Set<ContainerId> getContainerPreemptions() {
    return fungibleContainers;
  }

  /**
   * 获取可抢占资源请求列表
   * @return 可抢占资源请求列表
   */
  public List<ResourceRequest> getResourcePreemptions() {
    return fungibleResources;
  }

  /**
   * 获取NodeManager认证令牌列表
   * @return NM令牌列表
   */
  public List<NMToken> getNMTokens() {
    return nmTokens;
  }
  
  /**
   * 获取资源增加的容器列表（弹性调整）
   * @return 资源增加容器列表
   */
  public List<Container> getIncreasedContainers() {
    return increasedContainers;
  }
  
  /**
   * 获取资源减少的容器列表（弹性调整）
   * @return 资源减少容器列表
   */
  public List<Container> getDecreasedContainers() {
    return decreasedContainers;
  }

  /**
   * 获取优先级提升的容器列表
   * @return 优先级提升容器列表
   */
  public List<Container> getPromotedContainers() {
    return promotedContainers;
  }

  /**
   * 获取优先级降低的容器列表
   * @return 优先级降低容器列表
   */
  public List<Container> getDemotedContainers() {
    return demotedContainers;
  }

  /**
   * 获取上一次尝试保留的容器列表
   * @return 上一次尝试保留容器列表
   */
  public List<Container> getPreviousAttemptContainers() {
    return previousAttemptContainers;
  }

  /**
   * 获取被拒绝的调度请求列表
   * @return 被拒绝的调度请求列表
   */
  public List<RejectedSchedulingRequest> getRejectedRequest() {
    return rejectedRequest;
  }

  /**
   * 设置资源上限，仅用于测试
   * @param resource 新的资源上限
   */
  @VisibleForTesting
  public void setResourceLimit(Resource resource) {
    this.resourceLimit = resource;
  }

  @Override
  public String toString() {
    return "Allocation{" + "containers=" + containers + ", strictContainers="
        + strictContainers + ", fungibleContainers=" + fungibleContainers
        + ", fungibleResources=" + fungibleResources + ", nmTokens=" + nmTokens
        + ", increasedContainers=" + increasedContainers
        + ", decreasedContainers=" + decreasedContainers
        + ", promotedContainers=" + promotedContainers + ", demotedContainers="
        + demotedContainers + ", previousAttemptContainers="
        + previousAttemptContainers + ", resourceLimit=" + resourceLimit + '}';
  }
}