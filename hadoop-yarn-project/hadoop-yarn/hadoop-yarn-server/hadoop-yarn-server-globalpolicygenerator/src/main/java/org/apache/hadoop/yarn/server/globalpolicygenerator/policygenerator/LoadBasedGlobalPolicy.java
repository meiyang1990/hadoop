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

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MIN_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MAX_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_PENDING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_MAX_EDIT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_EDIT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_GPG_LOAD_BASED_SCALING;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_GPG_LOAD_BASED_SCALING;

/**
 * 基于负载的全局策略实现，根据子集群待处理应用数量将集群负载映射为0.0-1.0的权重，生成加权路由策略
 * 用于YARN联邦环境中根据子集群负载动态调整调度权重，实现负载均衡
 */
public class LoadBasedGlobalPolicy extends GlobalPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedGlobalPolicy.class);

  /** 权重缩放算法枚举 */
  public enum Scaling {
    LINEAR,
    QUADRATIC,
    LOG,
    NONE
  }

  // 开始降低权重的最小待处理应用数阈值
  private int minPending;
  // 停止降低权重的最大待处理应用数阈值，超过该阈值后权重将被设为最小值
  private int maxPending;
  // 子集群可被分配的最小权重值
  private float minWeight;
  // 单次可同时调整权重的最大子集群数量
  private int maxEdit;
  // 权重缩放算法类型
  private Scaling scaling = Scaling.NONE;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    // 从配置加载最小待处理应用阈值
    minPending = conf.getInt(FEDERATION_GPG_LOAD_BASED_MIN_PENDING,
        DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_PENDING);
    // 从配置加载最大待处理应用阈值
    maxPending = conf.getInt(FEDERATION_GPG_LOAD_BASED_MAX_PENDING,
        DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_PENDING);
    // 从配置加载最小权重值
    minWeight = conf.getFloat(FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT,
        DEFAULT_FEDERATION_GPG_LOAD_BASED_MIN_WEIGHT);
    // 从配置加载单次最大可调整数量
    maxEdit = conf.getInt(FEDERATION_GPG_LOAD_BASED_MAX_EDIT,
        DEFAULT_FEDERATION_GPG_LOAD_BASED_MAX_EDIT);

    try {
      // 解析配置的权重缩放算法类型
      scaling = Scaling.valueOf(conf.get(FEDERATION_GPG_LOAD_BASED_SCALING,
          DEFAULT_FEDERATION_GPG_LOAD_BASED_SCALING));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid scaling mode provided", e);
    }

    // 校验所有配置值合法性
    if (!(minPending <= maxPending)) {
      throw new YarnRuntimeException("minPending = " + minPending
          + " must be less than or equal to maxPending=" + maxPending);
    }
    if (!(minWeight >= 0 && minWeight < 1)) {
      throw new YarnRuntimeException(
          "minWeight = " + minWeight + " must be within range [0,1)");
    }
  }

  @Override
  protected Map<Class<?>, String> registerPaths() {
    // 注册需要获取的集群指标信息对应的REST端点
    Map<Class<?>, String> map = new HashMap<>();
    map.put(ClusterMetricsInfo.class, RMWSConsts.METRICS);
    return map;
  }

  /**
   * 更新队列的联邦路由策略，根据子集群负载动态生成新的权重策略
   *
   * @param queueName   队列名称
   * @param clusterInfo 子集群元信息映射，存储各子集群上报的指标数据
   * @param currentManager 当前队列已有的策略管理器，为空则新建
   *
   * @return 更新后的联邦策略管理器
   */
  @Override
  protected FederationPolicyManager updatePolicy(String queueName,
      Map<SubClusterId, Map<Class, Object>> clusterInfo,
      FederationPolicyManager currentManager) {
    if (currentManager == null) {
      LOG.info("Creating load based weighted policy queue {}.", queueName);
      currentManager = getWeightedLocalityPolicyManager(queueName, clusterInfo);
    } else if (currentManager instanceof WeightedLocalityPolicyManager) {
      LOG.info("Updating load based weighted policy queue {}.", queueName);
      currentManager = getWeightedLocalityPolicyManager(queueName, clusterInfo);
    } else {
      LOG.warn("Policy for queue {} is of type {}, expected {}.", queueName,
          currentManager.getClass(), WeightedLocalityPolicyManager.class);
    }
    return currentManager;
  }

  /**
   * 根据子集群负载指标生成加权位置策略管理器，自动计算各子集群权重
   *
   * @param queue 队列名称
   * @param subClusterMetricInfos 各子集群的指标信息映射
   * @return 生成好的加权位置策略管理器
   */
  protected WeightedLocalityPolicyManager getWeightedLocalityPolicyManager(String queue,
      Map<SubClusterId, Map<Class, Object>> subClusterMetricInfos) {

    // 解析提取各子集群的指标信息
    Map<SubClusterId, ClusterMetricsInfo> clusterMetrics =
        getSubClustersMetricsInfo(subClusterMetricInfos);

    if (MapUtils.isEmpty(clusterMetrics)) {
      return null;
    }

    // 计算目标权重并设置到策略管理器中
    WeightedLocalityPolicyManager manager = new WeightedLocalityPolicyManager();
    Map<SubClusterIdInfo, Float> weights = getTargetWeights(clusterMetrics);
    manager.setQueue(queue);
    // 权重同时用于AM分配和路由器策略
    manager.getWeightedPolicyInfo().setAMRMPolicyWeights(weights);
    manager.getWeightedPolicyInfo().setRouterPolicyWeights(weights);
    return manager;
  }

  /**
   * 从原始子集群信息中提取出集群指标信息
   *
   * @param subClusterMetricsInfo 原始子集群信息映射
   * @return 子集群到指标信息的映射
   */
  protected Map<SubClusterId, ClusterMetricsInfo> getSubClustersMetricsInfo(
      Map<SubClusterId, Map<Class, Object>> subClusterMetricsInfo) {

    // 检查输入是否为空
    if(MapUtils.isEmpty(subClusterMetricsInfo)) {
      LOG.warn("The metric info of the subCluster is empty.");
      return null;
    }

    Map<SubClusterId, ClusterMetricsInfo> clusterMetrics = new HashMap<>();
    // 遍历所有子集群提取指标信息
    for (Map.Entry<SubClusterId, Map<Class, Object>> entry : subClusterMetricsInfo.entrySet()) {
      SubClusterId subClusterId = entry.getKey();
      Map<Class, Object> subClusterMetrics = entry.getValue();
      ClusterMetricsInfo clusterMetricsInfo = (ClusterMetricsInfo)
          subClusterMetrics.getOrDefault(ClusterMetricsInfo.class, null);
      clusterMetrics.put(subClusterId, clusterMetricsInfo);
    }

    return clusterMetrics;
  }

  /**
   * 根据各子集群待处理负载计算目标权重
   *
   * @param clusterMetrics 各子集群指标信息
   * @return 子集群对应的目标权重映射
   */
  @VisibleForTesting
  protected Map<SubClusterIdInfo, Float> getTargetWeights(
      Map<SubClusterId, ClusterMetricsInfo> clusterMetrics) {
    // 初始化为均匀权重（所有子集群权重均为1）
    Map<SubClusterIdInfo, Float> weights = GPGUtils.createUniformWeights(clusterMetrics.keySet());

    List<SubClusterId> scs = new ArrayList<>(clusterMetrics.keySet());
    // 按待处理应用数降序排序子集群
    scs.sort(new SortByDescendingLoad(clusterMetrics));

    // 只取负载最高的前N个子集群调整权重
    scs = scs.subList(0, Math.min(maxEdit, scs.size()));

    // 遍历计算每个子集群最终权重
    for (SubClusterId sc : scs) {
      LOG.info("Updating weight for sub cluster {}", sc.toString());
      int pending = clusterMetrics.get(sc).getAppsPending();
      if (pending <= minPending) {
        // 待处理数低于最小阈值，不调整权重（保持初始1.0）
        LOG.info("Load ({}) is lower than minimum ({}), skipping", pending, minPending);
      } else if (pending < maxPending) {
        // 待处理数在阈值区间内，根据缩放算法计算权重
        // 将待处理数转换为[1, maxVal]区间，简化后续计算
        int val = pending - minPending;
        int maxVal = maxPending - minPending;

        // 根据缩放算法计算权重并映射到[minWeight, 1.0]区间
        float weight = getWeightByScaling(maxVal, val);
        weight = weight * (1.0f - minWeight);
        weight += minWeight;
        weights.put(new SubClusterIdInfo(sc), weight);
        LOG.info("Load ({}) is within maximum ({}), setting weights via {} "
            + "scale to {}", pending, maxPending, scaling, weight);
      } else {
        // 待处理数超过最大阈值，直接设置为最小权重
        weights.put(new SubClusterIdInfo(sc), minWeight);
        LOG.info("Load ({}) exceeded maximum ({}), setting weight to minimum: {}",
            pending, maxPending, minWeight);
      }
    }
    // 校验避免所有权重都为0的异常情况
    validateWeights(weights);
    return weights;
  }

  /**
   * 根据选定的缩放算法计算基础权重值
   *
   * @param maxPendingVal 最大区间值 = maxPending - minPending
   * @param curPendingVal 当前区间值 = pending - minPending
   * @return 计算得到的基础权重值，范围在[0, 1]
   */
  protected float getWeightByScaling(int maxPendingVal, int curPendingVal) {
    float weight = 1.0f;
    switch (scaling) {
    case NONE:
      // 不缩放，保持权重为1
      break;
    case LINEAR:
      // 线性缩放：负载越高权重越低，线性下降
      weight = (float) (maxPendingVal - curPendingVal) / (float) (maxPendingVal);
      break;
    case QUADRATIC:
      // 二次缩放：低负载下降慢，高负载下降快，更快降低高负载子集群权重
      double maxValQuad = Math.pow(maxPendingVal, 2);
      double valQuad = Math.pow(curPendingVal, 2);
      weight = (float) (maxValQuad - valQuad) / (float) (maxValQuad);
      break;
    case LOG:
      // 对数缩放：低负载下降快，高负载下降慢，对低负载更敏感
      double maxValLog = Math.log(maxPendingVal);
      double valLog = Math.log(curPendingVal);
      weight = (float) (maxValLog - valLog) / (float) (maxValLog);
      break;
    default:
      LOG.warn("No suitable scaling found, Skip.");
      break;
    }
    return weight;
  }

  /**
   * 校验权重合法性，避免出现所有权重全为0的异常情况，若全0则重置为全1
   * @param weights 需要校验的权重映射
   */
  private void validateWeights(Map<SubClusterIdInfo, Float> weights) {
    for(Float w : weights.values()) {
      // 只要存在一个非零权重，校验通过
      if(w > 0.0f) {
        return;
      }
    }
    LOG.warn("All {} generated weights were 0.0f. Resetting to 1.0f.", weights.size());
    // 所有权重都是0，重置为1，保证集群可用
    weights.replaceAll((i, v) -> 1.0f);
  }

  /**
   * 按子集群待处理应用数降序排序的比较器
   */
  private static final class SortByDescendingLoad
      implements Comparator<SubClusterId> {

    private Map<SubClusterId, ClusterMetricsInfo> clusterMetrics;

    private SortByDescendingLoad(
        Map<SubClusterId, ClusterMetricsInfo> clusterMetrics) {
      this.clusterMetrics = clusterMetrics;
    }

    public int compare(SubClusterId a, SubClusterId b) {
      // 按待处理应用数降序排序
      return clusterMetrics.get(b).getAppsPending() - clusterMetrics.get(a)
          .getAppsPending();
    }
  }
}