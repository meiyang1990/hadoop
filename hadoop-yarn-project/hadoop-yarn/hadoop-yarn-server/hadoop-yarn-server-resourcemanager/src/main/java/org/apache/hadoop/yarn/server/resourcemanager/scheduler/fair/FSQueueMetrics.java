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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.FSQueueMetricsForCustomResources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

/**
 * 公平调度器队列指标收集类，扩展基础队列指标，增加公平调度特有的资源份额指标
 * 支持自定义资源类型的指标收集，为监控系统提供队列资源使用与分配统计数据
 */
@Metrics(context="yarn")
public class FSQueueMetrics extends QueueMetrics {

  @Metric("Fair share of memory in MB") MutableGaugeLong fairShareMB;
  @Metric("Fair share of CPU in vcores") MutableGaugeLong fairShareVCores;
  @Metric("Steady fair share of memory in MB") MutableGaugeLong steadyFairShareMB;
  @Metric("Steady fair share of CPU in vcores") MutableGaugeLong steadyFairShareVCores;
  @Metric("Minimum share of memory in MB") MutableGaugeLong minShareMB;
  @Metric("Minimum share of CPU in vcores") MutableGaugeLong minShareVCores;
  @Metric("Maximum share of memory in MB") MutableGaugeLong maxShareMB;
  @Metric("Maximum share of CPU in vcores") MutableGaugeLong maxShareVCores;
  @Metric("Maximum number of applications") MutableGaugeInt maxApps;
  @Metric("Maximum AM share of memory in MB") MutableGaugeLong maxAMShareMB;
  @Metric("Maximum AM share of CPU in vcores") MutableGaugeInt maxAMShareVCores;
  @Metric("AM resource usage of memory in MB") MutableGaugeLong amResourceUsageMB;
  @Metric("AM resource usage of CPU in vcores") MutableGaugeInt amResourceUsageVCores;

  // 自定义资源类型指标容器，仅当存在超过内存和CPU的自定义资源时初始化
  private final FSQueueMetricsForCustomResources customResources;
  // 当前队列使用的调度策略名称
  private String schedulingPolicy;

  /**
   * Constructor for {@link FairScheduler} queue metrics data object.
   *
   * @param ms the MetricSystem to register with
   * @param queueName the queue name
   * @param parent the parent {@link Queue}
   * @param enableUserMetrics store metrics on user level
   * @param conf the {@link Configuration} object to build buckets upon
   */
  FSQueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);

    // 如果存在除内存和CPU外的自定义资源类型，初始化自定义资源指标容器
    if (ResourceUtils.getNumberOfKnownResourceTypes() > 2) {
      this.customResources =
          new FSQueueMetricsForCustomResources();
    } else {
      this.customResources = null;
    }
  }
  
  long getFairShareMB() {
    return fairShareMB.value();
  }
  
  long getFairShareVirtualCores() {
    return fairShareVCores.value();
  }

  /**
   * 获取队列即时公平份额资源总量，包含自定义资源类型
   *
   * @return 队列即时公平份额资源对象
   */
  public Resource getFairShare() {
    if (customResources != null) {
      return Resource.newInstance(fairShareMB.value(),
          (int) fairShareVCores.value(),
          customResources.getFairShareValues());
    }
    return Resource.newInstance(fairShareMB.value(),
        (int) fairShareVCores.value());
  }

  /**
   * 设置队列即时公平份额资源总量，支持包含自定义资源
   *
   * @param resource 待设置的即时公平份额资源对象
   */
  public void setFairShare(Resource resource) {
    fairShareMB.set(resource.getMemorySize());
    fairShareVCores.set(resource.getVirtualCores());
    if (customResources != null) {
      customResources.setFairShare(resource);
    }
  }

  public long getSteadyFairShareMB() {
    return steadyFairShareMB.value();
  }

  public long getSteadyFairShareVCores() {
    return steadyFairShareVCores.value();
  }

  /**
   * 获取队列稳定公平份额资源总量，包含自定义资源类型
   *
   * @return 队列稳定公平份额资源对象
   */
  public Resource getSteadyFairShare() {
    if (customResources != null) {
      return Resource.newInstance(steadyFairShareMB.value(),
          (int) steadyFairShareVCores.value(),
          customResources.getSteadyFairShareValues());
    }
    return Resource.newInstance(steadyFairShareMB.value(),
        (int) steadyFairShareVCores.value());
  }

  /**
   * 设置队列稳定公平份额资源总量，支持包含自定义资源
   *
   * @param resource 待设置的稳定公平份额资源对象
   */
  public void setSteadyFairShare(Resource resource) {
    steadyFairShareMB.set(resource.getMemorySize());
    steadyFairShareVCores.set(resource.getVirtualCores());
    if (customResources != null) {
      customResources.setSteadyFairShare(resource);
    }
  }

  public long getMinShareMB() {
    return minShareMB.value();
  }
  
  public long getMinShareVirtualCores() {
    return minShareVCores.value();
  }

  /**
   * 获取队列最小保证资源份额总量，包含自定义资源类型
   *
   * @return 队列最小保证资源对象
   */
  public Resource getMinShare() {
    if (customResources != null) {
      return Resource.newInstance(minShareMB.value(),
          (int) minShareVCores.value(),
          customResources.getMinShareValues());
    }
    return Resource.newInstance(minShareMB.value(),
        (int) minShareVCores.value());
  }

  /**
   * 设置队列最小保证资源份额总量，支持包含自定义资源
   *
   * @param resource 待设置的最小保证资源对象
   */
  public void setMinShare(Resource resource) {
    minShareMB.set(resource.getMemorySize());
    minShareVCores.set(resource.getVirtualCores());
    if (customResources != null) {
      customResources.setMinShare(resource);
    }
  }
  
  public long getMaxShareMB() {
    return maxShareMB.value();
  }
  
  public long getMaxShareVirtualCores() {
    return maxShareVCores.value();
  }

  /**
   * 获取队列最大允许资源份额总量，包含自定义资源类型
   *
   * @return 队列最大允许资源对象
   */
  public Resource getMaxShare() {
    if (customResources != null) {
      return Resource.newInstance(maxShareMB.value(),
          (int) maxShareVCores.value(),
          customResources.getMaxShareValues());
    }
    return Resource.newInstance(maxShareMB.value(),
        (int) maxShareVCores.value());
  }

  /**
   * 设置队列最大允许资源份额总量，支持包含自定义资源
   *
   * @param resource 待设置的最大允许资源对象
   */
  public void setMaxShare(Resource resource) {
    maxShareMB.set(resource.getMemorySize());
    maxShareVCores.set(resource.getVirtualCores());
    if (customResources != null) {
      customResources.setMaxShare(resource);
    }
  }

  public int getMaxApps() {
    return maxApps.value();
  }

  public void setMaxApps(int max) {
    maxApps.set(max);
  }

  /**
   * 获取AM允许使用的最大内存量，单位MB
   *
   * @return AM允许使用的最大内存量
   */
  public long getMaxAMShareMB() {
    return maxAMShareMB.value();
  }

  /**
   * 获取AM允许使用的最大CPU核数
   *
   * @return AM允许使用的最大CPU核数
   */
  public int getMaxAMShareVCores() {
    return maxAMShareVCores.value();
  }

  /**
   * 获取AM允许使用的最大资源总量，包含自定义资源类型
   *
   * @return AM允许使用的最大资源对象
   */
  public Resource getMaxAMShare() {
    if (customResources != null) {
      return Resource.newInstance(maxAMShareMB.value(),
          maxAMShareVCores.value(),
          customResources.getMaxAMShareValues());
    }
    return Resource.newInstance(maxAMShareMB.value(),
        maxAMShareVCores.value());
  }

  /**
   * 设置AM允许使用的最大资源总量，支持包含自定义资源
   *
   * @param resource 待设置的AM最大资源对象
   */
  public void setMaxAMShare(Resource resource) {
    maxAMShareMB.set(resource.getMemorySize());
    maxAMShareVCores.set(resource.getVirtualCores());
    if (customResources != null) {
      customResources.setMaxAMShare(resource);
    }
  }

  /**
   * 获取当前已使用的AM内存量，单位MB
   *
   * @return 已使用AM内存量
   */
  public long getAMResourceUsageMB() {
    return amResourceUsageMB.value();
  }

  /**
   * 获取当前已使用的AM CPU核数
   *
   * @return 已使用AM CPU核数
   */
  public int getAMResourceUsageVCores() {
    return amResourceUsageVCores.value();
  }

  /**
   * 获取当前已使用的AM资源总量，包含自定义资源类型
   *
   * @return 已使用AM资源对象
   */
  public Resource getAMResourceUsage() {
    if (customResources != null) {
      return Resource.newInstance(amResourceUsageMB.value(),
          amResourceUsageVCores.value(),
          customResources.getAMResourceUsageValues());
    }
    return Resource.newInstance(amResourceUsageMB.value(),
        amResourceUsageVCores.value());
  }

  /**
   * 设置当前已使用的AM资源总量，支持包含自定义资源
   *
   * @param resource 待设置的已使用AM资源对象
   */
  public void setAMResourceUsage(Resource resource) {
    amResourceUsageMB.set(resource.getMemorySize());
    amResourceUsageVCores.set(resource.getVirtualCores());
    if (customResources != null) {
      customResources.setAMResourceUsage(resource);
    }
  }

  /**
   * 获取当前队列的调度策略名称
   *
   * @return 调度策略名称
   */
  @Metric("Scheduling policy")
  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }

  public void setSchedulingPolicy(String policy) {
    schedulingPolicy = policy;
  }

  /**
   * 获取指定队列的指标对象，使用默认指标系统
   * 如果不存在则创建并注册新指标对象
   *
   * @param queueName 队列名称
   * @param parent 父队列
   * @param enableUserMetrics 是否启用用户级指标
   * @param conf 配置对象
   * @return 队列指标对象
   */
  public synchronized
  static FSQueueMetrics forQueue(String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return forQueue(ms, queueName, parent, enableUserMetrics, conf);
  }

  /**
   * 获取指定队列的指标对象，使用指定指标系统
   * 如果不存在则创建并注册新指标对象
   *
   * @param ms 指标系统实例
   * @param queueName 队列名称
   * @param parent 父队列
   * @param enableUserMetrics 是否启用用户级指标
   * @param conf 配置对象
   * @return 队列指标对象
   */
  @VisibleForTesting
  public synchronized
  static FSQueueMetrics forQueue(MetricsSystem ms, String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    // 从缓存中查找已有指标对象
    QueueMetrics metrics = QueueMetrics.getQueueMetrics().get(queueName);
    if (metrics == null) {
      // 创建新的公平调度队列指标对象
      metrics = new FSQueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
          .tag(QUEUE_INFO, queueName);

      // 注册到指标系统
      if (ms != null) {
        metrics = ms.register(
            sourceName(queueName).toString(),
            "Metrics for queue: " + queueName, metrics);
      }
      // 将新指标存入缓存
      QueueMetrics.getQueueMetrics().put(queueName, metrics);
    }

    return (FSQueueMetrics)metrics;
  }

  FSQueueMetricsForCustomResources getCustomResources() {
    return customResources;
  }
}