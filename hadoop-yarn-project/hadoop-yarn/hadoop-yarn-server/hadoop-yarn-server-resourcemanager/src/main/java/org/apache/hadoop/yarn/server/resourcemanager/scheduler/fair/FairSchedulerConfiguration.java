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

import static org.apache.hadoop.yarn.util.resource.ResourceUtils.RESOURCE_REQUEST_VALUE_PATTERN;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 公平调度器配置类，封装公平调度器所有配置项的读取和解析逻辑
 */
@Private
@Evolving
public class FairSchedulerConfiguration extends Configuration {

  public static final Logger LOG = LoggerFactory.getLogger(
      FairSchedulerConfiguration.class.getName());

  /**
   * Resource Increment request grant-able by the FairScheduler.
   * This property is looked up in the yarn-site.xml.
   * @deprecated The preferred way to configure the increment is by using the
   * yarn.resource-types.{RESOURCE_NAME}.increment-allocation property,
   * for memory: yarn.resource-types.memory-mb.increment-allocation
   */
  @Deprecated
  public static final String RM_SCHEDULER_INCREMENT_ALLOCATION_MB =
    YarnConfiguration.YARN_PREFIX + "scheduler.increment-allocation-mb";
  @Deprecated
  public static final int DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_MB = 1024;
  /**
   * Resource Increment request grant-able by the FairScheduler.
   * This property is looked up in the yarn-site.xml.
   * @deprecated The preferred way to configure the increment is by using the
   * yarn.resource-types.{RESOURCE_NAME}.increment-allocation property,
   * for CPU: yarn.resource-types.vcores.increment-allocation
   */
  @Deprecated
  public static final String RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES =
    YarnConfiguration.YARN_PREFIX + "scheduler.increment-allocation-vcores";
  @Deprecated
  public static final int DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES = 1;

  /** Threshold for container size for making a container reservation as a
   * multiple of increment allocation. Only container sizes above this are
   * allowed to reserve a node */
  public static final String
      RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE =
      YarnConfiguration.YARN_PREFIX +
          "scheduler.reservation-threshold.increment-multiple";
  public static final float
      DEFAULT_RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE = 2f;

  private static final String CONF_PREFIX =  "yarn.scheduler.fair.";

  /**
   * Used during FS-&gt;CS conversion. When enabled, background threads are
   * not started. This property should NOT be used by end-users!
   */
  public static final String MIGRATION_MODE = CONF_PREFIX + "migration.mode";

  /**
   * Disables checking whether a placement rule is terminal or not. Only
   * used during migration mode. This property should NOT be used by end users!
   */
  public static final String NO_TERMINAL_RULE_CHECK = CONF_PREFIX +
      "no-terminal-rule.check";

  public static final String ALLOCATION_FILE = CONF_PREFIX + "allocation.file";
  protected static final String DEFAULT_ALLOCATION_FILE = "fair-scheduler.xml";
  
  /** Whether pools can be created that were not specified in the FS configuration file
   */
  public static final String ALLOW_UNDECLARED_POOLS = CONF_PREFIX +
      "allow-undeclared-pools";
  public static final boolean DEFAULT_ALLOW_UNDECLARED_POOLS = true;
  
  /** Whether to use the user name as the queue name (instead of "default") if
   * the request does not specify a queue. */
  public static final String  USER_AS_DEFAULT_QUEUE = CONF_PREFIX +
      "user-as-default-queue";
  public static final boolean DEFAULT_USER_AS_DEFAULT_QUEUE = true;

  protected static final float  DEFAULT_LOCALITY_THRESHOLD = -1.0f;

  /** Cluster threshold for node locality. */
  public static final String LOCALITY_THRESHOLD_NODE = CONF_PREFIX +
      "locality.threshold.node";
  public static final float  DEFAULT_LOCALITY_THRESHOLD_NODE =
		  DEFAULT_LOCALITY_THRESHOLD;

  /** Cluster threshold for rack locality. */
  public static final String LOCALITY_THRESHOLD_RACK = CONF_PREFIX +
      "locality.threshold.rack";
  public static final float  DEFAULT_LOCALITY_THRESHOLD_RACK =
		  DEFAULT_LOCALITY_THRESHOLD;

  /**
   * Delay for node locality.
   * @deprecated Continuous scheduling is known to cause locking issue inside
   * Only used when {@link #CONTINUOUS_SCHEDULING_ENABLED} is enabled
   */
  @Deprecated
  protected static final String LOCALITY_DELAY_NODE_MS = CONF_PREFIX +
      "locality-delay-node-ms";
  @Deprecated
  protected static final long DEFAULT_LOCALITY_DELAY_NODE_MS = -1L;

  /**
   * Delay for rack locality.
   * @deprecated Continuous scheduling is known to cause locking issue inside
   * Only used when {@link #CONTINUOUS_SCHEDULING_ENABLED} is enabled
   */
  @Deprecated
  protected static final String LOCALITY_DELAY_RACK_MS = CONF_PREFIX +
      "locality-delay-rack-ms";
  @Deprecated
  protected static final long DEFAULT_LOCALITY_DELAY_RACK_MS = -1L;

  /**
   * Enable continuous scheduling or not.
   * @deprecated Continuous scheduling is known to cause locking issue inside
   * the scheduler in larger cluster, more than 100 nodes, use
   * {@link #ASSIGN_MULTIPLE} to improve  container allocation ramp up.
   */
  @Deprecated
  public static final String CONTINUOUS_SCHEDULING_ENABLED = CONF_PREFIX +
      "continuous-scheduling-enabled";
  @Deprecated
  public static final boolean DEFAULT_CONTINUOUS_SCHEDULING_ENABLED = false;

  /**
   * Sleep time of each pass in continuous scheduling (5ms in default).
   * @deprecated Continuous scheduling is known to cause locking issue inside
   * Only used when {@link #CONTINUOUS_SCHEDULING_ENABLED} is enabled
   */
  @Deprecated
  public static final String CONTINUOUS_SCHEDULING_SLEEP_MS = CONF_PREFIX +
      "continuous-scheduling-sleep-ms";
  @Deprecated
  public static final int DEFAULT_CONTINUOUS_SCHEDULING_SLEEP_MS = 5;

  /** Whether preemption is enabled. */
  public static final String  PREEMPTION = CONF_PREFIX + "preemption";
  public static final boolean DEFAULT_PREEMPTION = false;

  protected static final String AM_PREEMPTION =
      CONF_PREFIX + "am.preemption";
  protected static final String AM_PREEMPTION_PREFIX =
          CONF_PREFIX + "am.preemption.";
  protected static final boolean DEFAULT_AM_PREEMPTION = true;

  protected static final String PREEMPTION_THRESHOLD =
      CONF_PREFIX + "preemption.cluster-utilization-threshold";
  protected static final float DEFAULT_PREEMPTION_THRESHOLD = 0.8f;

  public static final String WAIT_TIME_BEFORE_KILL = CONF_PREFIX +
      "waitTimeBeforeKill";
  public static final int DEFAULT_WAIT_TIME_BEFORE_KILL = 15000;

  /**
   * Postfix for resource allocation increments in the
   * yarn.resource-types.{RESOURCE_NAME}.increment-allocation property.
   */
  static final String INCREMENT_ALLOCATION = ".increment-allocation";

  /**
   * Configurable delay (ms) before an app's starvation is considered after
   * it is identified. This is to give the scheduler enough time to
   * allocate containers post preemption. This delay is added to the
   * {@link #WAIT_TIME_BEFORE_KILL} and enough heartbeats.
   *
   * This is intended to be a backdoor on production clusters, and hence
   * intentionally not documented.
   */
  public static final String WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS =
      CONF_PREFIX + "waitTimeBeforeNextStarvationCheck";
  public static final long
      DEFAULT_WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS = 10000;

  /** Whether to assign multiple containers in one check-in. */
  public static final String  ASSIGN_MULTIPLE = CONF_PREFIX + "assignmultiple";
  public static final boolean DEFAULT_ASSIGN_MULTIPLE = false;

  /** Whether to give more weight to apps requiring many resources. */
  public static final String  SIZE_BASED_WEIGHT = CONF_PREFIX +
      "sizebasedweight";
  public static final boolean DEFAULT_SIZE_BASED_WEIGHT = false;

  /** Maximum number of containers to assign on each check-in. */
  public static final String DYNAMIC_MAX_ASSIGN =
      CONF_PREFIX + "dynamic.max.assign";
  private static final boolean DEFAULT_DYNAMIC_MAX_ASSIGN = true;

  /**
   * Specify exact number of containers to assign on each heartbeat, if dynamic
   * max assign is turned off.
   */
  public static final String MAX_ASSIGN = CONF_PREFIX + "max.assign";
  public static final int DEFAULT_MAX_ASSIGN = -1;

  /** The update interval for calculating resources in FairScheduler .*/
  public static final String UPDATE_INTERVAL_MS =
      CONF_PREFIX + "update-interval-ms";
  public static final int DEFAULT_UPDATE_INTERVAL_MS = 500;

  /** Ratio of nodes available for an app to make an reservation on. */
  public static final String RESERVABLE_NODES =
          CONF_PREFIX + "reservable-nodes";
  public static final float RESERVABLE_NODES_DEFAULT = 0.05f;

  private static final String INVALID_RESOURCE_DEFINITION_PREFIX =
          "Error reading resource config--invalid resource definition: ";
  private static final String RESOURCE_PERCENTAGE_PATTERN =
      "^(-?(\\d+)(\\.\\d*)?)\\s*%\\s*";
  private static final String RESOURCE_VALUE_PATTERN =
      "^(-?\\d+)(\\.\\d*)?\\s*";
  /**
   * For resources separated by spaces instead of a comma.
   */
  private static final String RESOURCES_WITH_SPACES_PATTERN =
      "-?\\d+(?:\\.\\d*)?\\s*[a-z]+\\s*";

  public FairSchedulerConfiguration() {
    super();
  }
  
  public FairSchedulerConfiguration(Configuration conf) {
    super(conf);
  }

  /**
   * 获取最小资源分配量
   * @return 包含内存和CPU的最小资源对象
   */
  public Resource getMinimumAllocation() {
    int mem = getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int cpu = getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    return Resources.createResource(mem, cpu);
  }

  /**
   * 获取最大资源分配量
   * @return 包含内存和CPU的最大资源对象
   */
  public Resource getMaximumAllocation() {
    int mem = getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    int cpu = getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    return Resources.createResource(mem, cpu);
  }

  /**
   * 获取资源分配增量步长
   * @return 包含所有资源的增量步长对象
   */
  public Resource getIncrementAllocation() {
    Long memory = null;
    Integer vCores = null;
    // 存储除内存和CPU外的其他资源增量
    Map<String, Long> others = new HashMap<>();
    // 获取所有已定义资源类型
    ResourceInformation[] resourceTypes = ResourceUtils.getResourceTypesArray();
    // 遍历所有资源类型读取增量配置
    for (int i=0; i < resourceTypes.length; ++i) {
      String name = resourceTypes[i].getName();
      // 生成新格式配置键
      String propertyKey = getAllocationIncrementPropKey(name);
      String propValue = get(propertyKey);
      if (propValue != null) {
        // 匹配值+单位格式
        Matcher matcher = RESOURCE_REQUEST_VALUE_PATTERN.matcher(propValue);
        if (matcher.matches()) {
          // 解析数值和单位
          long value = Long.parseLong(matcher.group(1));
          String unit = matcher.group(2);
          // 转换为资源默认单位数值
          long valueInDefaultUnits = getValueInDefaultUnits(value, unit, name);
          others.put(name, valueInDefaultUnits);
        } else {
          throw new IllegalArgumentException("Property " + propertyKey +
              " is not in \"value [unit]\" format: " + propValue);
        }
      }
    }
    // 处理内存增量，兼容旧配置格式
    if (others.containsKey(ResourceInformation.MEMORY_MB.getName())) {
      memory = others.get(ResourceInformation.MEMORY_MB.getName());
      if (get(RM_SCHEDULER_INCREMENT_ALLOCATION_MB) != null) {
        String overridingKey = getAllocationIncrementPropKey(
                ResourceInformation.MEMORY_MB.getName());
        LOG.warn("Configuration " + overridingKey + "=" + get(overridingKey) +
            " is overriding the " + RM_SCHEDULER_INCREMENT_ALLOCATION_MB +
            "=" + get(RM_SCHEDULER_INCREMENT_ALLOCATION_MB) + " property");
      }
      others.remove(ResourceInformation.MEMORY_MB.getName());
    } else {
      memory = getLong(
          RM_SCHEDULER_INCREMENT_ALLOCATION_MB,
          DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_MB);
    }
    // 处理CPU增量，兼容旧配置格式
    if (others.containsKey(ResourceInformation.VCORES.getName())) {
      vCores = others.get(ResourceInformation.VCORES.getName()).intValue();
      if (get(RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES) != null) {
        String overridingKey = getAllocationIncrementPropKey(
            ResourceInformation.VCORES.getName());
        LOG.warn("Configuration " + overridingKey + "=" + get(overridingKey) +
            " is overriding the " + RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES +
            "=" + get(RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES) + " property");
      }
      others.remove(ResourceInformation.VCORES.getName());
    } else {
      vCores = getInt(
          RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES,
          DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES);
    }
    // 组装完整资源对象返回
    return Resource.newInstance(memory, vCores, others);
  }

  private long getValueInDefaultUnits(long value, String unit,
      String resourceName) {
    return unit.isEmpty() ? value : UnitsConversionUtil.convert(unit,
        ResourceUtils.getDefaultUnit(resourceName), value);
  }

  private String getAllocationIncrementPropKey(String resourceName) {
    return YarnConfiguration.RESOURCE_TYPES + "." + resourceName +
        INCREMENT_ALLOCATION;
  }

  /**
   * 获取容器预留阈值（按增量倍数计算）
   * @return 预留阈值倍数
   */
  public float getReservationThresholdIncrementMultiple() {
    return getFloat(
      RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE,
      DEFAULT_RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE);
  }

  /**
   * 获取节点局部性阈值
   * @return 节点局部性阈值
   */
  public float getLocalityThresholdNode() {
    return getFloat(LOCALITY_THRESHOLD_NODE