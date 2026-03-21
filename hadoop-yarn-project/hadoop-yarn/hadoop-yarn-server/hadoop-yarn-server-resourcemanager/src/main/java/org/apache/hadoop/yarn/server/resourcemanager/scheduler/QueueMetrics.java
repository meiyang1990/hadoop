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

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.metrics.CustomResourceMetricValue;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;

/**
 * YARN ResourceManager 调度队列指标收集类，负责统计队列各个维度的运行指标，并暴露给Hadoop metrics系统。
 * 支持按队列、用户、节点分区（节点标签）三个维度分层统计，包含应用状态、资源分配、抢占等各类指标。
 */
@InterfaceAudience.Private
@Metrics(context="yarn")
public class QueueMetrics implements MetricsSource {
  @Metric("# of apps submitted") MutableCounterInt appsSubmitted;
  @Metric("# of running apps") MutableGaugeInt appsRunning;
  @Metric("# of pending apps") MutableGaugeInt appsPending;
  @Metric("# of apps completed") MutableCounterInt appsCompleted;
  @Metric("# of apps killed") MutableCounterInt appsKilled;
  @Metric("# of apps failed") MutableCounterInt appsFailed;

  @Metric("# of Unmanaged apps submitted")
  private MutableCounterInt unmanagedAppsSubmitted;
  @Metric("# of Unmanaged running apps")
  private MutableGaugeInt unmanagedAppsRunning;
  @Metric("# of Unmanaged pending apps")
  private MutableGaugeInt unmanagedAppsPending;
  @Metric("# of Unmanaged apps completed")
  private MutableCounterInt unmanagedAppsCompleted;
  @Metric("# of Unmanaged apps killed")
  private MutableCounterInt unmanagedAppsKilled;
  @Metric("# of Unmanaged apps failed")
  private MutableCounterInt unmanagedAppsFailed;

  @Metric("Aggregate # of allocated node-local containers")
    MutableCounterLong aggregateNodeLocalContainersAllocated;
  @Metric("Aggregate # of allocated rack-local containers")
    MutableCounterLong aggregateRackLocalContainersAllocated;
  @Metric("Aggregate # of allocated off-switch containers")
    MutableCounterLong aggregateOffSwitchContainersAllocated;
  @Metric("Aggregate # of preempted containers") MutableCounterLong
      aggregateContainersPreempted;
  @Metric("Aggregate # of preempted memory seconds") MutableCounterLong
      aggregateMemoryMBSecondsPreempted;
  @Metric("Aggregate # of preempted vcore seconds") MutableCounterLong
      aggregateVcoreSecondsPreempted;
  @Metric("# of active users") MutableGaugeInt activeUsers;
  @Metric("# of active applications") MutableGaugeInt activeApplications;
  @Metric("App Attempt First Container Allocation Delay")
    MutableRate appAttemptFirstContainerAllocationDelay;
  @Metric("Aggregate total of preempted memory MB")
    MutableCounterLong aggregateMemoryMBPreempted;
  @Metric("Aggregate total of preempted vcores")
    MutableCounterLong aggregateVcoresPreempted;

  // 仅默认分区更新以下指标
  @Metric("Allocated memory in MB") MutableGaugeLong allocatedMB;
  @Metric("Allocated CPU in virtual cores") MutableGaugeInt allocatedVCores;
  @Metric("# of allocated containers") MutableGaugeInt allocatedContainers;
  @Metric("Aggregate # of allocated containers")
    MutableCounterLong aggregateContainersAllocated;
  @Metric("Aggregate # of released containers")
    MutableCounterLong aggregateContainersReleased;
  @Metric("Available memory in MB") MutableGaugeLong availableMB;
  @Metric("Available CPU in virtual cores") MutableGaugeInt availableVCores;
  @Metric("Pending memory allocation in MB") MutableGaugeLong pendingMB;
  @Metric("Pending CPU allocation in virtual cores")
    MutableGaugeInt pendingVCores;
  @Metric("# of pending containers") MutableGaugeInt pendingContainers;
  @Metric("# of reserved memory in MB") MutableGaugeLong reservedMB;
  @Metric("Reserved CPU in virtual cores") MutableGaugeInt reservedVCores;
  @Metric("# of reserved containers") MutableGaugeInt reservedContainers;

  // 内部配置验证模式标记，仅供内部使用
  private static final String CONFIGURATION_VALIDATION = "yarn.configuration-validation";

  private final MutableGaugeInt[] runningTime;
  private TimeBucketMetrics<ApplicationId> runBuckets;

  static final Logger LOG = LoggerFactory.getLogger(QueueMetrics.class);
  static final MetricsInfo RECORD_INFO = info("QueueMetrics",
      "Metrics for the resource scheduler");
  protected static final MetricsInfo QUEUE_INFO =
      info("Queue", "Metrics by queue");
  protected static final MetricsInfo USER_INFO =
      info("User", "Metrics by user");
  protected static final MetricsInfo PARTITION_INFO =
      info("Partition", "Metrics by partition");
  static final Splitter Q_SPLITTER =
      Splitter.on('.').omitEmptyStrings().trimResults();

  protected final MetricsRegistry registry;
  protected final String queueName;
  private QueueMetrics parent;
  private Queue parentQueue;
  protected final MetricsSystem metricsSystem;
  protected final Map<String, QueueMetrics> users;
  protected final Configuration conf;
  private QueueMetricsForCustomResources queueMetricsForCustomResources;

  private final boolean enableUserMetrics;

  protected static final MetricsInfo P_RECORD_INFO =
      info("PartitionQueueMetrics", "Metrics for the resource scheduler");

  // 内部默认分区名称，对应无标签场景
  public static final String DEFAULT_PARTITION = "default";

  // JMX注册时默认分区使用空字符串
  public static final String DEFAULT_PARTITION_JMX_STR = "";

  // 指标名称分隔符
  public static final String METRIC_NAME_DELIMITER = ".";

  private static final String ALLOCATED_RESOURCE_METRIC_PREFIX =
      "AllocatedResource.";
  private static final String ALLOCATED_RESOURCE_METRIC_DESC =
    "Allocated NAME";

  private static final String AVAILABLE_RESOURCE_METRIC_PREFIX =
    "AvailableResource.";
  private static final String AVAILABLE_RESOURCE_METRIC_DESC =
    "Available NAME";

  private static final String PENDING_RESOURCE_METRIC_PREFIX =
    "PendingResource.";
  private static final String PENDING_RESOURCE_METRIC_DESC =
    "Pending NAME";

  private static final String RESERVED_RESOURCE_METRIC_PREFIX =
    "ReservedResource.";
  private static final String RESERVED_RESOURCE_METRIC_DESC =
    "Reserved NAME";

  private static final String AGGREGATE_PREEMPTED_SECONDS_METRIC_PREFIX =
    "AggregatePreemptedSeconds.";
  private static final String AGGREGATE_PREEMPTED_SECONDS_METRIC_DESC =
    "Aggregate Preempted Seconds for NAME";
  protected Set<String> storedPartitionMetrics = Sets.newConcurrentHashSet();

  /**
   * 构造函数，创建队列指标实例。
   * @param ms 指标系统实例
   * @param queueName 队列名称
   * @param parent 父队列
   * @param enableUserMetrics 是否启用按用户维度指标统计
   * @param conf YARN配置
   */
  public QueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {

    if (this instanceof PartitionQueueMetrics) {
      registry = new MetricsRegistry(P_RECORD_INFO);
    } else {
      registry = new MetricsRegistry(RECORD_INFO);
    }
    this.queueName = queueName;

    this.parent = parent != null ? parent.getMetrics() : null;
    this.parentQueue = parent;
    this.users = enableUserMetrics ? new HashMap<String, QueueMetrics>() : null;
    this.enableUserMetrics = enableUserMetrics;

    metricsSystem = ms;
    this.conf = conf;
    runningTime = buildBuckets(conf);

    createQueueMetricsForCustomResources();
  }

  protected QueueMetrics tag(MetricsInfo info, String value) {
    registry.tag(info, value);
    return this;
  }

  protected static StringBuilder sourceName(String queueName) {
    StringBuilder sb = new StringBuilder(RECORD_INFO.name());
    int i = 0;
    // 按层级拆分队列名生成指标源名称
    for (String node : Q_SPLITTER.split(queueName)) {
      sb.append(",q").append(i++).append('=').append(node);
    }
    return sb;
  }

  static StringBuilder pSourceName(String partition) {
    StringBuilder sb = new StringBuilder(P_RECORD_INFO.name());
    sb.append(",partition").append('=').append(partition);
    return sb;
  }

  static StringBuilder qSourceName(String queueName) {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (String node : Q_SPLITTER.split(queueName)) {
      sb.append(",q").append(i++).append('=').append(node);
    }
    return sb;
  }

  /**
   * 获取队列指标实例，使用默认指标系统。
   */
  public synchronized static QueueMetrics forQueue(String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    return forQueue(DefaultMetricsSystem.instance(), queueName, parent,
        enableUserMetrics, conf);
  }

  /**
   * Helper method to clear cache.
   */
  @Private
  public synchronized static void clearQueueMetrics() {
    QUEUE_METRICS.clear();
  }

  /**
   * Simple metrics cache to help prevent re-registrations.
   */
  private static final Map<String, QueueMetrics> QUEUE_METRICS =
      new ConcurrentHashMap<>();

  /**
   * Returns the metrics cache to help prevent re-registrations.
   *
   * @return A string to {@link QueueMetrics} map.
   */
  public static Map<String, QueueMetrics> getQueueMetrics() {
    return QUEUE_METRICS;
  }

  /**
   * 获取或创建队列指标实例，使用指定指标系统。
   */
  public synchronized static QueueMetrics forQueue(MetricsSystem ms,
      String queueName, Queue parent, boolean enableUserMetrics,
      Configuration conf) {
    QueueMetrics metrics = getQueueMetrics().get(queueName);
    if (metrics == null) {
      metrics = new QueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
          .tag(QUEUE_INFO, queueName);

      // 注册到指标系统
      if (ms != null) {
        metrics = ms.register(sourceName(queueName).toString(),
            "Metrics for queue: " + queueName, metrics);
      }
      getQueueMetrics().put(queueName, metrics);
    }

    return metrics;
  }

  /**
   * 获取指定用户的指标实例，不存在则创建。
   */
  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }
    QueueMetrics metrics = users.get(userName);
    if (metrics == null) {
      metrics =
          new QueueMetrics(metricsSystem, queueName, null, false, conf);
      users.put(userName, metrics);
      // 注册用户维度指标到指标系统
      metricsSystem.register(
          sourceName(queueName).append(",user=").append(userName).toString(),
          "Metrics for user '"+ userName +"' in queue '"+ queueName +"'",
          metrics.tag(QUEUE_INFO, queueName).tag(USER_INFO, userName));
    }
    return metrics;
  }

  /**
   * 获取指定分区（节点标签）下当前队列的指标实例，不存在则创建。
   * 按分区*队列层级统计指标，支持节点标签分区的资源统计。
   * @param partition 节点分区（节点标签）
   * @return 分区队列指标实例
   */
  public synchronized QueueMetrics getPartitionQueueMetrics(String partition) {

    String partitionJMXStr = partition;

    if ((partition == null)
        || (partition.equals(RMNodeLabelsManager.NO_LABEL))) {
      partition = DEFAULT_PARTITION;
      partitionJMXStr = DEFAULT_PARTITION_JMX_STR;
    }

    String metricName = partition + METRIC_NAME_DELIMITER + this.queueName;
    QueueMetrics metrics = getQueueMetrics().get(metricName);

    if (metrics == null) {
      QueueMetrics queueMetrics =
          new PartitionQueueMetrics(metricsSystem, this.queueName, parentQueue,
              this.enableUserMetrics, this.conf, partition);
      // 注册分区队列指标到指标系统
      metricsSystem.register(
          pSourceName(partitionJMXStr).append(qSourceName(this.queueName))
              .toString(),
          "Metrics for queue: " + this.queueName,
          queueMetrics.tag(PARTITION_INFO, partitionJMXStr).tag(QUEUE_INFO,
              this.queueName));
      // 配置验证模式下不缓存指标
      if (!isConfigurationValidationSet(conf)) {
        getQueueMetrics().put(metricName, queueMetrics);
      }
      registerPartitionMetricsCreation(metricName);
      return queueMetrics;
    } else {
      return metrics;
    }
  }

  /**
   * Check whether we are in a configuration validation mode. INTERNAL ONLY.
   *
   * @param conf the configuration to check
   * @return true if
   */
  public static boolean isConfigurationValidationSet(Configuration conf) {
    return conf.getBoolean(CONFIGURATION_VALIDATION, false);
  }

  /**
   * Set configuration validation mode. INTERNAL ONLY.
   *
   * @param conf the configuration to update
   * @param value the value for the validation mode
   */
  public static void setConfigurationValidation(Configuration conf, boolean value) {
    conf.setBoolean(CONFIGURATION_VALIDATION, value);
  }

  /**
   * 获取指定分区（节点标签）的根层级指标实例，不存在则创建。
   * 按分区层级统计全队列聚合指标。
   * @param partition 节点分区（节点标签）
   * @return 分区指标实例
   */
  private QueueMetrics getPartitionMetrics(String partition) {

    String partitionJMXStr = partition;
    if ((partition == null)
        || (partition.equals(RMNodeLabelsManager.NO_LABEL))) {
      partition = DEFAULT_PARTITION;
      partitionJMXStr = DEFAULT_PARTITION_JMX_STR;
    }

    String metricName = partition + METRIC_NAME_DELIMITER;
    QueueMetrics metrics = getQueueMetrics().get(metricName);
    if (metrics == null) {
      metrics = new PartitionQueueMetrics(metricsSystem, this.queueName, null,
          false, this.conf, partition);

      // 注册分区指标到指标系统
      if (metricsSystem != null) {
        metricsSystem.register(pSourceName(partitionJMXStr).toString(),
            "Metrics for partition: " + partitionJMXStr,
            (PartitionQueueMetrics) metrics.tag(PARTITION_INFO,
                partitionJMXStr));
      }
      getQueueMetrics().put(metricName, metrics);
      registerPartitionMetricsCreation(metricName);
    }
    return metrics;
  }

  /**
   * 将逗号分隔的字符串解析为整数列表。
   */
  private ArrayList<Integer> parseInts(String value) {
    ArrayList<Integer> result = new