// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation;

import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueueType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 队列配置属性值对象，存储从allocation.xml配置文件解析得到的公平调度队列配置信息。
 * 由于配置项较多，需通过{@link Builder}构造器来构建实例。
 */
public class QueueProperties {
  // 临时存储解析得到的分配信息，只有整个分配文件解析成功后才会保存到最终字段
  /** 各队列最小资源分配，key为队列名，value为最小资源量 */
  private final Map<String, Resource> minQueueResources;
  /** 各队列最大资源分配，key为队列名，value为最大可配置资源量 */
  private final Map<String, ConfigurableResource> maxQueueResources;
  /** 各队列子队列最大资源分配，key为队列名，value为子队列最大可配置资源量 */
  private final Map<String, ConfigurableResource> maxChildQueueResources;
  /** 各队列最大同时运行应用数，key为队列名，value为最大应用数 */
  private final Map<String, Integer> queueMaxApps;
  /** 各队列ApplicationMaster最大资源占比，key为队列名，value为占比 */
  private final Map<String, Float> queueMaxAMShares;
  /** 各队列公平调度权重，key为队列名，value为权重值 */
  private final Map<String, Float> queueWeights;
  /** 各队列调度策略，key为队列名，value为调度策略实例 */
  private final Map<String, SchedulingPolicy> queuePolicies;
  /** 各队列最小资源抢占超时时间，key为队列名，value为超时时间（毫秒） */
  private final Map<String, Long> minSharePreemptionTimeouts;
  /** 各队列公平份额抢占超时时间，key为队列名，value为超时时间（毫秒） */
  private final Map<String, Long> fairSharePreemptionTimeouts;
  /** 各队列公平份额抢占阈值，key为队列名，value为阈值 */
  private final Map<String, Float> fairSharePreemptionThresholds;
  /** 各队列访问控制列表，key为队列名，第二层key为访问类型 */
  private final Map<String, Map<AccessType, AccessControlList>> queueAcls;
  /** 各队列预留访问控制列表，key为队列名，第二层key为预留ACL类型 */
  private final Map<String, Map<ReservationACL, AccessControlList>>
          reservationAcls;
  /** 支持资源预留的队列集合 */
  private final Set<String> reservableQueues;
  /** 不可被抢占的队列集合 */
  private final Set<String> nonPreemptableQueues;
  /** 按类型分类的已配置队列集合，key为队列类型（叶子/父队列） */
  private final Map<FSQueueType, Set<String>> configuredQueues;
  /** 各队列单容器最大分配资源，key为队列名，value为最大资源量 */
  private final Map<String, Resource> queueMaxContainerAllocation;

  /**
   * 从Builder构造QueueProperties实例，仅Builder可调用
   */
  QueueProperties(Builder builder) {
    this.reservableQueues = builder.reservableQueues;
    this.minQueueResources = builder.minQueueResources;
    this.fairSharePreemptionTimeouts = builder.fairSharePreemptionTimeouts;
    this.queueWeights = builder.queueWeights;
    this.nonPreemptableQueues = builder.nonPreemptableQueues;
    this.configuredQueues = builder.configuredQueues;
    this.queueMaxAMShares = builder.queueMaxAMShares;
    this.queuePolicies = builder.queuePolicies;
    this.fairSharePreemptionThresholds = builder.fairSharePreemptionThresholds;
    this.queueMaxApps = builder.queueMaxApps;
    this.minSharePreemptionTimeouts = builder.minSharePreemptionTimeouts;
    this.maxQueueResources = builder.maxQueueResources;
    this.maxChildQueueResources = builder.maxChildQueueResources;
    this.reservationAcls = builder.reservationAcls;
    this.queueAcls = builder.queueAcls;
    this.queueMaxContainerAllocation = builder.queueMaxContainerAllocation;
  }

  public Map<FSQueueType, Set<String>> getConfiguredQueues() {
    return configuredQueues;
  }

  public Map<String, Long> getMinSharePreemptionTimeouts() {
    return minSharePreemptionTimeouts;
  }

  public Map<String, Long> getFairSharePreemptionTimeouts() {
    return fairSharePreemptionTimeouts;
  }

  public Map<String, Float> getFairSharePreemptionThresholds() {
    return fairSharePreemptionThresholds;
  }

  public Map<String, Resource> getMinQueueResources() {
    return minQueueResources;
  }

  public Map<String, ConfigurableResource> getMaxQueueResources() {
    return maxQueueResources;
  }

  public Map<String, ConfigurableResource> getMaxChildQueueResources() {
    return maxChildQueueResources;
  }

  public Map<String, Integer> getQueueMaxApps() {
    return queueMaxApps;
  }

  public Map<String, Float> getQueueWeights() {
    return queueWeights;
  }

  public Map<String, Float> getQueueMaxAMShares() {
    return queueMaxAMShares;
  }

  public Map<String, SchedulingPolicy> getQueuePolicies() {
    return queuePolicies;
  }

  public Map<String, Map<AccessType, AccessControlList>> getQueueAcls() {
    return queueAcls;
  }

  public Map<String, Map<ReservationACL, AccessControlList>>
      getReservationAcls() {
    return reservationAcls;
  }

  public Set<String> getReservableQueues() {
    return reservableQueues;
  }

  public Set<String> getNonPreemptableQueues() {
    return nonPreemptableQueues;
  }

  public Map<String, Resource> getMaxContainerAllocation() {
    return queueMaxContainerAllocation;
  }

  /**
   * {@link QueueProperties}的构造器类，用于逐步构建队列配置属性实例。
   * 大部分方法都是按队列名将属性添加到构造器的Map中，除了一些查询类方法如
   * {@link #isAclDefinedForAccessType(String, AccessType)}和getter方法。
   */
  public static final class Builder {
    private Map<String, Resource> minQueueResources = new HashMap<>();
    private Map<String, ConfigurableResource> maxQueueResources =
        new HashMap<>();
    private Map<String, ConfigurableResource> maxChildQueueResources =
        new HashMap<>();
    private Map<String, Integer> queueMaxApps = new HashMap<>();
    private Map<String, Float> queueMaxAMShares = new HashMap<>();
    private Map<String, Resource> queueMaxContainerAllocation = new HashMap<>();
    private Map<String, Float> queueWeights = new HashMap<>();
    private Map<String, SchedulingPolicy> queuePolicies = new HashMap<>();
    private Map<String, Long> minSharePreemptionTimeouts = new HashMap<>();
    private Map<String, Long> fairSharePreemptionTimeouts = new HashMap<>();
    private Map<String, Float> fairSharePreemptionThresholds = new HashMap<>();
    private Map<String, Map<AccessType, AccessControlList>> queueAcls =
        new HashMap<>();
    private Map<String, Map<ReservationACL, AccessControlList>>
            reservationAcls = new HashMap<>();
    private Set<String> reservableQueues = new HashSet<>();
    private Set<String> nonPreemptableQueues = new HashSet<>();
    // 记录所有已配置队列名，供Web UI等展示使用
    // 配置队列按叶子队列/父队列类型分类，该信息用于队列创建
    private Map<FSQueueType, Set<String>> configuredQueues = new HashMap<>();

    /**
     * 构造器初始化，按队列类型分类创建空的队列集合
     */
    Builder() {
      for (FSQueueType queueType : FSQueueType.values()) {
        configuredQueues.put(queueType, new HashSet<>());
      }
    }

    /**
     * 创建新的构造器实例
     * @return 空构造器实例
     */
    public static Builder create() {
      return new Builder();
    }

    /**
     * 添加队列最小资源配置
     */
    public Builder minQueueResources(String queueName, Resource resource) {
      this.minQueueResources.put(queueName, resource);
      return this;
    }

    /**
     * 添加队列最大资源配置
     */
    public Builder maxQueueResources(String queueName,
        ConfigurableResource resource) {
      this.maxQueueResources.put(queueName, resource);
      return this;
    }

    /**
     * 添加队列子队列最大资源配置
     */
    public Builder maxChildQueueResources(String queueName,
        ConfigurableResource resource) {
      this.maxChildQueueResources.put(queueName, resource);
      return this;
    }

    /**
     * 添加队列最大运行应用数配置
     */
    public Builder queueMaxApps(String queueName, int value) {
      this.queueMaxApps.put(queueName, value);
      return this;
    }

    /**
     * 添加队列AM最大资源占比配置
     */
    public Builder queueMaxAMShares(String queueName, float value) {
      this.queueMaxAMShares.put(queueName, value);
      return this;
    }

    /**
     * 添加队列调度权重配置
     */
    public Builder queueWeights(String queueName, float value) {
      this.queueWeights.put(queueName, value);
      return this;
    }

    /**
     * 添加队列调度策略配置
     */
    public Builder queuePolicies(String queueName, SchedulingPolicy policy) {
      this.queuePolicies.put(queueName, policy);
      return this;
    }

    /**
     * 添加队列最小资源抢占超时配置
     */
    public Builder minSharePreemptionTimeouts(String queueName, long value) {
      this.minSharePreemptionTimeouts.put(queueName, value);
      return this;
    }

    /**
     * 添加队列公平份额抢占超时配置
     */
    public Builder fairSharePreemptionTimeouts(String queueName, long value) {
      this.fairSharePreemptionTimeouts.put(queueName, value);
      return this;
    }

    /**
     * 添加队列公平份额抢占阈值配置
     */
    public Builder fairSharePreemptionThresholds(String queueName,
        float value) {
      this.fairSharePreemptionThresholds.put(queueName, value);
      return this;
    }

    /**
     * 添加队列访问ACL配置
     */
    public Builder queueAcls(String queueName, AccessType accessType,
        AccessControlList acls) {
      this.queueAcls.putIfAbsent(queueName, new HashMap<>());
      this.queueAcls.get(queueName).put(accessType, acls);
      return this;
    }

    /**
     * 添加队列预留ACL配置
     */
    public Builder reservationAcls(String queueName,
        ReservationACL reservationACL, AccessControlList acls) {
      this.reservationAcls.putIfAbsent(queueName, new HashMap<>());
      this.reservationAcls.get(queueName).put(reservationACL, acls);
      return this;
    }

    /**
     * 将队列标记为支持资源预留
     */
    public Builder reservableQueues(String queue) {
      this.reservableQueues.add(queue);
      return this;
    }

    /**
     * 将队列标记为不可抢占
     */
    public Builder nonPreemptableQueues(String queue) {
      this.nonPreemptableQueues.add(queue);
      return this;
    }

    /**
     * 添加队列单容器最大分配资源配置
     */
    public Builder queueMaxContainerAllocation(String queueName,
        Resource value) {
      queueMaxContainerAllocation.put(queueName, value);
      return this;
    }

    /**
     * 添加已配置队列到对应类型分类中
     */
    public void configuredQueues(FSQueueType queueType, String queueName) {
      this.configuredQueues.get(queueType).add(queueName);
    }

    /**
     * 检查指定队列的指定访问类型是否已配置ACL
     * @param queueName 队列名
     * @param accessType 访问类型
     * @return 是否配置了ACL
     */
    public boolean isAclDefinedForAccessType(String queueName,
        AccessType accessType) {
      Map<AccessType, AccessControlList> aclsForQueue =
          this.queueAcls.get(queueName);
      return aclsForQueue != null && aclsForQueue.get(accessType) != null;
    }

    public Map<String, Resource> getMinQueueResources() {
      return minQueueResources;
    }

    public Map<String, ConfigurableResource> getMaxQueueResources() {
      return maxQueueResources;
    }

    /**
     * 构建最终的QueueProperties实例
     * @return 填充完成的队列配置属性对象
     */
    public QueueProperties build() {
      return new QueueProperties(this);
    }
  }
}