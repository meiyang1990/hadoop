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
package org.apache.hadoop.hdfs.server.balancer;

import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * HDFS数据均衡工具参数容器，存储均衡过程所需的所有配置参数。
 * 使用Builder模式构建，支持灵活配置各项均衡参数。
 */
@InterfaceAudience.Private
final class BalancerParameters {
  private final BalancingPolicy policy;
  private final double threshold;
  private final int maxIdleIteration;
  private final long hotBlockTimeInterval;
  /** Exclude the nodes in this set. */
  private final Set<String> excludedNodes;
  /** If empty, include any node; otherwise, include only these nodes. */
  private final Set<String> includedNodes;
  /**
   * If empty, any node can be a source; otherwise, use only these nodes as
   * source nodes.
   */
  private final Set<String> sourceNodes;
  /**
   * If empty, any node can be a source; otherwise, these nodes will be excluded as
   * source nodes.
   */
  private final Set<String> excludedSourceNodes;
  /**
   * If empty, any node can be a target; otherwise, use only these nodes as
   * target nodes.
   */
  private final Set<String> targetNodes;
  /**
   * If empty, any node can be a target; otherwise, these nodes will be excluded as
   * target nodes.
   */
  private final Set<String> excludedTargetNodes;
  /**
   * A set of block pools to run the balancer on.
   */
  private final Set<String> blockpools;
  /**
   * Whether to run the balancer during upgrade.
   */
  private final boolean runDuringUpgrade;

  private final boolean runAsService;

  private final boolean sortTopNodes;

  private final int limitOverUtilizedNum;

  /** 默认参数实例 */
  static final BalancerParameters DEFAULT = new BalancerParameters();

  private BalancerParameters() {
    this(new Builder());
  }

  /**
   * 从Builder构建参数实例
   * @param builder 参数构建器
   */
  private BalancerParameters(Builder builder) {
    this.policy = builder.policy;
    this.threshold = builder.threshold;
    this.maxIdleIteration = builder.maxIdleIteration;
    this.excludedNodes = builder.excludedNodes;
    this.includedNodes = builder.includedNodes;
    this.sourceNodes = builder.sourceNodes;
    this.excludedSourceNodes = builder.excludedSourceNodes;
    this.targetNodes = builder.targetNodes;
    this.excludedTargetNodes = builder.excludedTargetNodes;
    this.blockpools = builder.blockpools;
    this.runDuringUpgrade = builder.runDuringUpgrade;
    this.runAsService = builder.runAsService;
    this.sortTopNodes = builder.sortTopNodes;
    this.limitOverUtilizedNum = builder.limitOverUtilizedNum;
    this.hotBlockTimeInterval = builder.hotBlockTimeInterval;
  }

  /**
   * 获取数据均衡策略
   * @return 均衡策略实例
   */
  BalancingPolicy getBalancingPolicy() {
    return this.policy;
  }

  /**
   * 获取均衡阈值，当节点间使用率差异超过该阈值时才会进行块移动
   * @return 均衡阈值（百分比）
   */
  double getThreshold() {
    return this.threshold;
  }

  /**
   * 获取最大空闲迭代次数，达到该次数后均衡结束
   * @return 最大空闲迭代次数
   */
  int getMaxIdleIteration() {
    return this.maxIdleIteration;
  }

  /**
   * 获取需要排除的节点集合，均衡过程不会涉及这些节点
   * @return 排除节点ID集合
   */
  Set<String> getExcludedNodes() {
    return this.excludedNodes;
  }

  /**
   * 获取需要包含的节点集合，仅对这些节点进行均衡
   * @return 包含节点ID集合
   */
  Set<String> getIncludedNodes() {
    return this.includedNodes;
  }

  /**
   * 获取允许作为块数据源的节点集合
   * @return 数据源节点ID集合
   */
  Set<String> getSourceNodes() {
    return this.sourceNodes;
  }

  /**
   * 获取禁止作为块数据源的节点集合
   * @return 排除的数据源节点ID集合
   */
  Set<String> getExcludedSourceNodes() {
    return this.excludedSourceNodes;
  }

  /**
   * 获取允许作为块目标的节点集合
   * @return 数据目标节点ID集合
   */
  Set<String> getTargetNodes() {
    return this.targetNodes;
  }

  /**
   * 获取禁止作为块目标的节点集合
   * @return 排除的数据目标节点ID集合
   */
  Set<String> getExcludedTargetNodes() {
    return this.excludedTargetNodes;
  }

  /**
   * 获取需要进行均衡的块池集合
   * @return 块池ID集合
   */
  Set<String> getBlockPools() {
    return this.blockpools;
  }

  /**
   * 获取是否在集群升级过程中允许运行均衡
   * @return true表示允许升级时运行均衡
   */
  boolean getRunDuringUpgrade() {
    return this.runDuringUpgrade;
  }

  /**
   * 获取是否以服务模式持续运行均衡
   * @return true表示以服务模式运行
   */
  boolean getRunAsService() {
    return this.runAsService;
  }

  /**
   * 获取是否对过载节点按使用率排序后处理
   * @return true表示需要排序
   */
  boolean getSortTopNodes() {
    return this.sortTopNodes;
  }

  /**
   * 获取每次迭代处理的最大过载节点数量限制
   * @return 最大过载节点数量
   */
  int getLimitOverUtilizedNum() {
    return this.limitOverUtilizedNum;
  }

  /**
   * 获取热块识别时间间隔，仅移动该间隔内访问过的块
   * @return 热块时间间隔（毫秒）
   */
  long getHotBlockTimeInterval() {
    return this.hotBlockTimeInterval;
  }

  @Override
  public String toString() {
    return String.format("%s.%s [%s," + " threshold = %s,"
        + " max idle iteration = %s," + " #excluded nodes = %s,"
        + " #included nodes = %s," + " #source nodes = %s,"
        + " #excluded source nodes = %s," + " #target nodes = %s,"
        + " #excluded target nodes = %s,"
        + " #blockpools = %s," + " run during upgrade = %s,"
        + " sort top nodes = %s," + " limit overUtilized nodes num = %s,"
        + " hot block time interval = %s]",
        Balancer.class.getSimpleName(), getClass().getSimpleName(), policy,
        threshold, maxIdleIteration, excludedNodes.size(),
        includedNodes.size(), sourceNodes.size(), excludedSourceNodes.size(), targetNodes.size(),
        excludedTargetNodes.size(), blockpools.size(),
        runDuringUpgrade, sortTopNodes, limitOverUtilizedNum, hotBlockTimeInterval);
  }

  /**
   * BalancerParameters构建器，使用Builder模式构建参数实例
   */
  static class Builder {
    // 默认参数值
    private BalancingPolicy policy = BalancingPolicy.Node.INSTANCE;
    private double threshold = 10.0;
    private int maxIdleIteration =
        NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS;
    private Set<String> excludedNodes = Collections.<String> emptySet();
    private Set<String> includedNodes = Collections.<String> emptySet();
    private Set<String> sourceNodes = Collections.<String> emptySet();
    private Set<String> excludedSourceNodes = Collections.<String> emptySet();
    private Set<String> targetNodes = Collections.<String> emptySet();
    private Set<String> excludedTargetNodes = Collections.<String> emptySet();
    private Set<String> blockpools = Collections.<String> emptySet();
    private boolean runDuringUpgrade = false;
    private boolean runAsService = false;
    private boolean sortTopNodes = false;
    private int limitOverUtilizedNum = Integer.MAX_VALUE;
    private long hotBlockTimeInterval = 0;

    Builder() {
    }

    /**
     * 设置均衡策略
     * @param p 均衡策略实例
     * @return 当前构建器
     */
    Builder setBalancingPolicy(BalancingPolicy p) {
      this.policy = p;
      return this;
    }

    /**
     * 设置均衡阈值
     * @param t 阈值（百分比）
     * @return 当前构建器
     */
    Builder setThreshold(double t) {
      this.threshold = t;
      return this;
    }

    /**
     * 设置最大空闲迭代次数
     * @param m 最大空闲迭代次数
     * @return 当前构建器
     */
    Builder setMaxIdleIteration(int m) {
      this.maxIdleIteration = m;
      return this;
    }

    /**
     * 设置热块识别时间间隔
     * @param t 时间间隔（毫秒）
     * @return 当前构建器
     */
    Builder setHotBlockTimeInterval(long t) {
      this.hotBlockTimeInterval = t;
      return this;
    }

    /**
     * 设置排除节点集合
     * @param nodes 排除节点ID集合
     * @return 当前构建器
     */
    Builder setExcludedNodes(Set<String> nodes) {
      this.excludedNodes = nodes;
      return this;
    }

    /**
     * 设置包含节点集合
     * @param nodes 包含节点ID集合
     * @return 当前构建器
     */
    Builder setIncludedNodes(Set<String> nodes) {
      this.includedNodes = nodes;
      return this;
    }

    /**
     * 设置允许的数据源节点集合
     * @param nodes 数据源节点ID集合
     * @return 当前构建器
     */
    Builder setSourceNodes(Set<String> nodes) {
      this.sourceNodes = nodes;
      return this;
    }

    /**
     * 设置排除的数据源节点集合
     * @param nodes 排除的数据源节点ID集合
     * @return 当前构建器
     */
    Builder setExcludedSourceNodes(Set<String> nodes) {
      this.excludedSourceNodes = nodes;
      return this;
    }

    /**
     * 设置允许的数据目标节点集合
     * @param nodes 数据目标节点ID集合
     * @return 当前构建器
     */
    Builder setTargetNodes(Set<String> nodes) {
      this.targetNodes = nodes;
      return this;
    }

    /**
     * 设置排除的数据目标节点集合
     * @param nodes 排除的数据目标节点ID集合
     * @return 当前构建器
     */
    Builder setExcludedTargetNodes(Set<String> nodes) {
      this.excludedTargetNodes = nodes;
      return this;
    }

    /**
     * 设置需要均衡的块池集合
     * @param pools 块池ID集合
     * @return 当前构建器
     */
    Builder setBlockpools(Set<String> pools) {
      this.blockpools = pools;
      return this;
    }

    /**
     * 设置是否允许升级时运行均衡
     * @param run true表示允许
     * @return 当前构建器
     */
    Builder setRunDuringUpgrade(boolean run) {
      this.runDuringUpgrade = run;
      return this;
    }

    /**
     * 设置是否以服务模式运行
     * @param asService true表示作为服务持续运行
     * @return 当前构建器
     */
    Builder setRunAsService(boolean asService) {
      this.runAsService = asService;
      return this;
    }

    /**
     * 设置是否对过载节点排序
     * @param shouldSortTopNodes true表示需要排序
     * @return 当前构建器
     */
    Builder setSortTopNodes(boolean shouldSortTopNodes) {
      this.sortTopNodes = shouldSortTopNodes;
      return this;
    }

    /**
     * 设置每次迭代处理的最大过载节点数量
     * @param overUtilizedNum 最大数量
     * @return 当前构建器
     */
    Builder setLimitOverUtilizedNum(int overUtilizedNum) {
      this.limitOverUtilizedNum = overUtilizedNum;
      return this;
    }

    /**
     * 构建BalancerParameters实例
     * @return 构建完成的参数实例
     */
    BalancerParameters build() {
      return new BalancerParameters(this);
    }
  }
}