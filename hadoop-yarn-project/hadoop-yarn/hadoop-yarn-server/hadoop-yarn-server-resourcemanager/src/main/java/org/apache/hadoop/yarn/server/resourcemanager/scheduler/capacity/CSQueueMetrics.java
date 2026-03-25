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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.metrics2.lib.MutableGaugeFloat;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.CSQueueMetricsForCustomResources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

/**
 * 容量调度器队列指标收集类，扩展基础QueueMetrics，收集容量调度器特有的队列指标
 */
@Metrics(context = "yarn")
public class CSQueueMetrics extends QueueMetrics {

  // 仅在默认分区更新这些指标
  @Metric("AM memory limit in MB")
  MutableGaugeLong AMResourceLimitMB;
  @Metric("AM CPU limit in virtual cores")
  MutableGaugeLong AMResourceLimitVCores;
  @Metric("Used AM memory limit in MB")
  MutableGaugeLong usedAMResourceMB;
  @Metric("Used AM CPU limit in virtual cores")
  MutableGaugeLong usedAMResourceVCores;
  @Metric("Percent of Capacity Used")
  MutableGaugeFloat usedCapacity;
  @Metric("Percent of Absolute Capacity Used")
  MutableGaugeFloat absoluteUsedCapacity;
  @Metric("Guaranteed memory in MB")
  MutableGaugeLong guaranteedMB;
  @Metric("Guaranteed CPU in virtual cores")
  MutableGaugeInt guaranteedVCores;
  @Metric("Maximum memory in MB")
  MutableGaugeLong maxCapacityMB;
  @Metric("Maximum CPU in virtual cores")
  MutableGaugeInt maxCapacityVCores;
  @Metric("Guaranteed capacity in percentage relative to parent")
  private MutableGaugeFloat guaranteedCapacity;
  @Metric("Guaranteed capacity in percentage relative to total partition")
  private MutableGaugeFloat guaranteedAbsoluteCapacity;
  @Metric("Maximum capacity in percentage relative to parent")
  private MutableGaugeFloat maxCapacity;
  @Metric("Maximum capacity in percentage relative to total partition")
  private MutableGaugeFloat maxAbsoluteCapacity;

  private static final String GUARANTEED_CAPACITY_METRIC_PREFIX =
      "GuaranteedCapacity.";
  private static final String GUARANTEED_CAPACITY_METRIC_DESC =
      "GuaranteedCapacity of NAME";

  private static final String MAX_CAPACITY_METRIC_PREFIX =
      "MaxCapacity.";
  private static final String MAX_CAPACITY_METRIC_DESC =
      "MaxCapacity of NAME";

  // 自定义资源指标处理器
  private CSQueueMetricsForCustomResources csQueueMetricsForCustomResources;

  CSQueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);
  }

  /**
   * 初始化时注册所有自定义资源指标，所有自定义资源指标初始化为0
   */
  protected void registerCustomResources() {
    // 初始化并获取所有自定义资源列表
    Map<String, Long> customResources =
        csQueueMetricsForCustomResources.initAndGetCustomResources();
    // 注册保证容量自定义资源指标
    csQueueMetricsForCustomResources
        .registerCustomResources(customResources, this.registry,
            GUARANTEED_CAPACITY_METRIC_PREFIX, GUARANTEED_CAPACITY_METRIC_DESC);
    // 注册最大容量自定义资源指标
    csQueueMetricsForCustomResources
        .registerCustomResources(customResources, this.registry,
            MAX_CAPACITY_METRIC_PREFIX, MAX_CAPACITY_METRIC_DESC);
    super.registerCustomResources();
  }

  public long getAMResourceLimitMB() {
    return AMResourceLimitMB.value();
  }

  public long getAMResourceLimitVCores() {
    return AMResourceLimitVCores.value();
  }

  public long getUsedAMResourceMB() {
    return usedAMResourceMB.value();
  }

  public long getUsedAMResourceVCores() {
    return usedAMResourceVCores.value();
  }

  /**
   * 设置AM资源限额，仅对默认分区生效
   */
  public void setAMResouceLimit(String partition, Resource res) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      AMResourceLimitMB.set(res.getMemorySize());
      AMResourceLimitVCores.set(res.getVirtualCores());
    }
  }

  /**
   * 设置队列对应用户的AM资源限额
   */
  public void setAMResouceLimitForUser(String partition,
      String user, Resource res) {
    CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.setAMResouceLimit(partition, res);
    }
  }

  /**
   * 增加已使用AM资源计数，仅对默认分区生效
   */
  public void incAMUsed(String partition, String user, Resource res) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      usedAMResourceMB.incr(res.getMemorySize());
      usedAMResourceVCores.incr(res.getVirtualCores());
      CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
      if (userMetrics != null) {
        userMetrics.incAMUsed(partition, user, res);
      }
    }
  }

  /**
   * 减少已使用AM资源计数，仅对默认分区生效
   */
  public void decAMUsed(String partition, String user, Resource res) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      usedAMResourceMB.decr(res.getMemorySize());
      usedAMResourceVCores.decr(res.getVirtualCores());
      CSQueueMetrics userMetrics = (CSQueueMetrics) getUserMetrics(user);
      if (userMetrics != null) {
        userMetrics.decAMUsed(partition, user, res);
      }
    }
  }

  public float getUsedCapacity() {
    return usedCapacity.value();
  }

  /**
   * 设置已使用容量百分比，仅对默认分区生效
   */
  public void setUsedCapacity(String partition, float usedCap) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      this.usedCapacity.set(usedCap);
    }
  }

  public float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity.value();
  }

  /**
   * 设置绝对已使用容量百分比，仅对默认分区生效
   */
  public void setAbsoluteUsedCapacity(String partition,
      Float absoluteUsedCap) {
    if(partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      this.absoluteUsedCapacity.set(absoluteUsedCap);
    }
  }

  public long getGuaranteedMB() {
    return guaranteedMB.value();
  }

  public int getGuaranteedVCores() {
    return guaranteedVCores.value();
  }

  /**
   * 设置队列保证资源量，仅对默认分区生效
   */
  public void setGuaranteedResources(String partition, Resource res) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      guaranteedMB.set(res.getMemorySize());
      guaranteedVCores.set(res.getVirtualCores());
      // 如果存在自定义资源，更新自定义资源保证容量指标
      if (csQueueMetricsForCustomResources != null) {
        csQueueMetricsForCustomResources.setGuaranteedCapacity(res);
        csQueueMetricsForCustomResources.registerCustomResources(
            csQueueMetricsForCustomResources.getGuaranteedCapacity(), registry,
            GUARANTEED_CAPACITY_METRIC_PREFIX, GUARANTEED_CAPACITY_METRIC_DESC);
      }
    }
  }

  public long getMaxCapacityMB() {
    return maxCapacityMB.value();
  }

  public int getMaxCapacityVCores() {
    return maxCapacityVCores.value();
  }

  /**
   * 设置队列最大资源量，仅对默认分区生效
   */
  public void setMaxCapacityResources(String partition, Resource res) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      maxCapacityMB.set(res.getMemorySize());
      maxCapacityVCores.set(res.getVirtualCores());
      // 如果存在自定义资源，更新自定义资源最大容量指标
      if (csQueueMetricsForCustomResources != null) {
        csQueueMetricsForCustomResources.setMaxCapacity(res);
        csQueueMetricsForCustomResources.registerCustomResources(
            csQueueMetricsForCustomResources.getMaxCapacity(), registry,
            MAX_CAPACITY_METRIC_PREFIX, MAX_CAPACITY_METRIC_DESC);
      }
    }
  }

  @Override
  protected void createQueueMetricsForCustomResources() {
    // 如果存在除内存和CPU外的自定义资源类型，初始化自定义资源指标
    if (ResourceUtils.getNumberOfKnownResourceTypes() > 2) {
      this.csQueueMetricsForCustomResources =
          new CSQueueMetricsForCustomResources();
      setQueueMetricsForCustomResources(csQueueMetricsForCustomResources);
      registerCustomResources();
    }
  }

  /**
   * 空实现指标系统，用于配置验证阶段避免注册真实指标
   */
  @Metrics(context="dummymetricssystem")
  public static class DummyMetricsSystemImpl extends MetricsSystem {
    @Override
    public MetricsSystem init(String prefix) {
      return this;
    }

    @Override
    public <T> T register(String name, String desc, T source) {
      MetricsAnnotations.newSourceBuilder(source).build();
      return source;
    }

    @Override
    public void unregisterSource(String name) {
    }

    @Override
    public MetricsSource getSource(String name) {
      return null;
    }

    @Override
    public <T extends MetricsSink> T register(String name, String desc, T sink) {
      return null;
    }

    @Override
    public void register(Callback callback) {
    }

    @Override
    public void publishMetricsNow() {
    }

    @Override
    public boolean shutdown() {
      return false;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void startMetricsMBeans() {
    }

    @Override
    public void stopMetricsMBeans() {
    }

    @Override
    public String currentConfig() {
      return null;
    }
  }

  /**
   * 获取或创建指定队列的指标实例，已创建则复用，不存在则新建
   */
  public synchronized static CSQueueMetrics forQueue(String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    // 判断是否是配置验证阶段
    final boolean isConfigValidation = isConfigurationValidationSet(conf);

    // 配置验证阶段使用空指标系统，不真实注册
    MetricsSystem ms = isConfigValidation
        ? new DummyMetricsSystemImpl() : DefaultMetricsSystem.instance();
    QueueMetrics metrics = getQueueMetrics().get(queueName);
    if (metrics == null) {
      metrics =
          new CSQueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
              .tag(QUEUE_INFO, queueName);

      // 注册到指标系统
      if (ms != null) {
        metrics =
            ms.register(sourceName(queueName).toString(), "Metrics for queue: "
                + queueName, metrics);
      }

      // 非配置验证阶段缓存指标实例
      if (!isConfigValidation) {
        getQueueMetrics().put(queueName, metrics);
      }
    }

    return (CSQueueMetrics) metrics;
  }

  @Override
  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }
    CSQueueMetrics metrics = (CSQueueMetrics) users.get(userName);
    if (metrics == null) {
      metrics =
        new CSQueueMetrics(metricsSystem, queueName, null, false, conf);
      users.put(userName, metrics);
      // 注册用户级指标到指标系统
      metricsSystem.register(
          sourceName(queueName).append(",user=").append(userName).toString(),
          "Metrics for user '" + userName + "' in queue '" + queueName + "'",
          ((CSQueueMetrics) metrics.tag(QUEUE_INFO, queueName)).tag(USER_INFO,
              userName));
    }
    return metrics;
  }

  public float getGuaranteedCapacity() {
    return guaranteedCapacity.value();
  }

  public float getGuaranteedAbsoluteCapacity() {
    return guaranteedAbsoluteCapacity.value();
  }

  /**
   * 设置队列保证容量百分比，仅对默认分区生效
   */
  public void setGuaranteedCapacities(String partition, float capacity,
      float absoluteCapacity) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      guaranteedCapacity.set(capacity);
      guaranteedAbsoluteCapacity.set(absoluteCapacity);
    }
  }

  public float getMaxCapacity() {
    return maxCapacity.value();
  }

  public float getMaxAbsoluteCapacity() {
    return maxAbsoluteCapacity.value();
  }

  /**
   * 设置队列最大容量百分比，仅对默认分区生效
   */
  public void setMaxCapacities(String partition, float capacity,
      float absoluteCapacity) {
    if (partition == null || partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      maxCapacity.set(capacity);
      maxAbsoluteCapacity.set(absoluteCapacity);
    }
  }
}