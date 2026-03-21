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

package org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * YARN联邦全局策略生成器，定期从各子集群采集负载信息，生成并更新联邦路由策略到联邦状态存储。
 * 具体的策略更新逻辑由配置的GlobalPolicy实例实现。
 */
public class PolicyGenerator implements Runnable, Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(PolicyGenerator.class);

  // 全局策略生成器上下文，持有联邦状态存储、策略管理器门面等核心组件
  private GPGContext gpgContext;
  private Configuration conf;

  // 信息请求路径映射：key=返回数据类型，value=RM REST接口路径
  private Map<Class, String> pathMap = new HashMap<>();

  // 全局策略实例，实现具体的策略更新逻辑
  @VisibleForTesting
  private GlobalPolicy policy;

  /**
   * 构造PolicyGenerator实例，完成配置和初始化。
   *
   * @param conf 配置对象
   * @param context GPG上下文对象
   */
  public PolicyGenerator(Configuration conf, GPGContext context) {
    setConf(conf);
    init(context);
  }

  private void init(GPGContext context) {
    this.gpgContext = context;
    LOG.info("Initialized PolicyGenerator");
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    // 从配置反射创建全局策略实例
    this.policy = FederationStateStoreFacade.createInstance(conf,
        YarnConfiguration.GPG_GLOBAL_POLICY_CLASS,
        YarnConfiguration.DEFAULT_GPG_GLOBAL_POLICY_CLASS, GlobalPolicy.class);
    policy.setConf(conf);
    // 注册全局策略需要获取的RM接口路径
    pathMap.putAll(policy.registerPaths());
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public final void run() {
    Map<SubClusterId, SubClusterInfo> activeSubClusters;
    try {
      // 从联邦状态存储获取所有活跃子集群
      activeSubClusters = gpgContext.getStateStoreFacade().getSubClusters(true);
    } catch (YarnException e) {
      LOG.error("Error retrieving active sub-clusters", e);
      return;
    }

    // 从所有子集群获取调度器信息
    Map<SubClusterId, SchedulerInfo> schedInfo = getSchedulerInfo(activeSubClusters);

    // 提取所有队列名称，确保所有子集群调度器类型一致
    Set<String> queueNames = extractQueues(schedInfo);

    // 移除黑名单中的子集群
    activeSubClusters.keySet().removeAll(getBlackList());
    LOG.info("Active non-blacklist sub-clusters: {}",
        activeSubClusters.keySet());

    // 从非黑名单子集群获取所需的集群指标信息，用于后续负载计算
    Map<SubClusterId, Map<Class, Object>> clusterInfo =
        getInfos(activeSubClusters);

    // 遍历所有队列，更新策略到联邦状态存储
    for (String queueName : queueNames) {
      // 获取对应队列的策略管理器
      FederationPolicyManager manager;
      try {
        manager = this.gpgContext.getPolicyFacade().getPolicyManager(queueName);
      } catch (YarnException e) {
        LOG.error("GetPolicy for queue {} failed.", queueName, e);
        continue;
      }
      LOG.info("Updating policy for queue {}.", queueName);
      // 通过全局策略实例更新当前队列策略
      manager = policy.updatePolicy(queueName, clusterInfo, manager);
      try {
        // 将更新后的策略写回联邦状态存储
        this.gpgContext.getPolicyFacade().setPolicyManager(manager);
      } catch (YarnException e) {
        LOG.error("SetPolicy for queue {} failed.", queueName, e);
      }
    }
  }

  /**
   * 从各子集群RM的REST接口采集所需的指标信息。
   *
   * @param activeSubClusters 活跃子集群信息映射
   * @return 子集群ID -> (数据类型 -> 采集到的信息对象)
   */
  @VisibleForTesting
  protected Map<SubClusterId, Map<Class, Object>> getInfos(
      Map<SubClusterId, SubClusterInfo> activeSubClusters) {

    Map<SubClusterId, Map<Class, Object>> clusterInfo = new HashMap<>();
    // 遍历每个活跃子集群
    for (SubClusterInfo sci : activeSubClusters.values()) {
      // 遍历所有需要采集的接口
      for (Map.Entry<Class, String> e : this.pathMap.entrySet()) {
        // 初始化子集群信息存储容器
        if (!clusterInfo.containsKey(sci.getSubClusterId())) {
          clusterInfo.put(sci.getSubClusterId(), new HashMap<>());
        }
        // 调用RM WebService获取对应信息
        Object ret = GPGUtils.invokeRMWebService(sci.getRMWebServiceAddress(),
            e.getValue(), e.getKey(), conf);
        clusterInfo.get(sci.getSubClusterId()).put(e.getKey(), ret);
      }
    }

    return clusterInfo;
  }

  /**
   * 从各子集群RM获取调度器信息。
   *
   * @param activeSubClusters 活跃子集群信息映射
   * @return 子集群ID -> 调度器信息
   */
  @VisibleForTesting
  protected Map<SubClusterId, SchedulerInfo> getSchedulerInfo(
      Map<SubClusterId, SubClusterInfo> activeSubClusters) {
    Map<SubClusterId, SchedulerInfo> schedInfo =
        new HashMap<>();
    for (SubClusterInfo sci : activeSubClusters.values()) {
      // 调用RM调度器接口获取调度器类型信息
      SchedulerTypeInfo sti = GPGUtils
          .invokeRMWebService(sci.getRMWebServiceAddress(),
              RMWSConsts.SCHEDULER, SchedulerTypeInfo.class, conf);
      if(sti != null){
        schedInfo.put(sci.getSubClusterId(), sti.getSchedulerInfo());
      } else {
        LOG.warn("Skipped null scheduler info from SubCluster {}.", sci.getSubClusterId());
      }
    }
    return schedInfo;
  }

  /**
   * 从配置中读取黑名单子集群ID列表。
   *
   * @return 黑名单子集群ID集合
   */
  private Set<SubClusterId> getBlackList() {
    String blackListParam =
        conf.get(YarnConfiguration.GPG_POLICY_GENERATOR_BLACKLIST);
    if(blackListParam == null){
      return Collections.emptySet();
    }
    Set<SubClusterId> blackList = new HashSet<>();
    for (String id : blackListParam.split(",")) {
      blackList.add(SubClusterId.newInstance(id.trim()));
    }
    return blackList;
  }

  /**
   * 从所有子集群调度器信息中提取队列名称集合，仅支持容量调度器。
   *
   * @param schedInfo 各子集群调度器信息映射
   * @return 所有队列名称的并集
   */
  private Set<String> extractQueues(Map<SubClusterId, SchedulerInfo> schedInfo) {
    Set<String> queueNames = new HashSet<>();
    for (Map.Entry<SubClusterId, SchedulerInfo> entry : schedInfo.entrySet()) {
      if (entry.getValue() instanceof CapacitySchedulerInfo) {
        // 展平队列层级结构，提取所有非叶子队列名称
        queueNames.addAll(flattenQueue((CapacitySchedulerInfo) entry.getValue())
            .get(CapacitySchedulerQueueInfo.class));
      } else {
        LOG.warn("Skipping SubCluster {}, not configured with capacity scheduler.",
            entry.getKey());
      }
    }
    return queueNames;
  }

  /**
   * 展平容量调度器根队列层级结构，按队列类型分类收集队列名称。
   *
   * @param csi 根容量调度器信息
   * @return 类型 -> 队列名称集合 的映射
   */
  private Map<Class, Set<String>> flattenQueue(CapacitySchedulerInfo csi) {
    Map<Class, Set<String>> flattened = new HashMap<>();
    addOrAppend(flattened, csi.getClass(), csi.getQueueName());
    // 递归处理所有子队列
    for (CapacitySchedulerQueueInfo csqi : csi.getQueues().getQueueInfoList()) {
      flattenQueue(csqi, flattened);
    }
    return flattened;
  }

  /**
   * 递归展平队列层级结构，收集所有队列名称。
   *
   * @param csi 当前队列信息
   * @param flattened 结果存储映射
   */
  private void flattenQueue(CapacitySchedulerQueueInfo csi,
      Map<Class, Set<String>> flattened) {
    addOrAppend(flattened, csi.getClass(), csi.getQueueName());
    // 如果存在子队列继续递归处理
    if (csi.getQueues() != null) {
      for (CapacitySchedulerQueueInfo csqi : csi.getQueues().getQueueInfoList()) {
        flattenQueue(csqi, flattened);
      }
    }
  }

  /**
   * 辅助方法：向multimap中添加值，如果key不存在则初始化集合。
   *
   * @param multimap 多值map
   * @param key 键
   * @param value 值
   */
  private <K, V> void addOrAppend(Map<K, Set<V>> multimap, K key, V value) {
    if (!multimap.containsKey(key)) {
      multimap.put(key, new HashSet<>());
    }
    multimap.get(key).add(value);
  }

  public GlobalPolicy getPolicy() {
    return policy;
  }

  public void setPolicy(GlobalPolicy policy) {
    this.policy = policy;
  }
}