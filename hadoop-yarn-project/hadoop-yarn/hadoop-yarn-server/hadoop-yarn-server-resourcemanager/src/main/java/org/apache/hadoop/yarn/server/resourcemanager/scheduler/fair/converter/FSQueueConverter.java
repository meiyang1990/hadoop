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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion.CapacityConverter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion.CapacityConverterFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 将Fair Scheduler队列层次结构转换为Capacity Scheduler配置。
 * 用于从公平调度器迁移到容量调度器时自动完成配置转换。
 *
 */
public class FSQueueConverter {
  /** AM资源占比禁用标记值 */
  public static final float QUEUE_MAX_AM_SHARE_DISABLED = -1.0f;
  /** 最大运行应用数未设置标记 */
  private static final int MAX_RUNNING_APPS_UNSET = Integer.MAX_VALUE;
  /** 公平策略名称常量 */
  private static final String FAIR_POLICY = "fair";
  /** FIFO策略名称常量 */
  private static final String FIFO_POLICY = "fifo";

  /** 规则处理处理器，用于处理不兼容配置并输出警告/错误 */
  private final FSConfigToCSConfigRuleHandler ruleHandler;
  /** 容量调度器配置对象，用于写入转换后的配置 */
  private CapacitySchedulerConfiguration capacitySchedulerConfig;
  /** 全局抢占是否启用 */
  private final boolean preemptionEnabled;
  /** 是否启用基于大小的权重计算 */
  private final boolean sizeBasedWeight;
  /** 集群总资源 */
  @SuppressWarnings("unused")
  private final Resource clusterResource;
  /** 队列AM资源占比默认值 */
  private final float queueMaxAMShareDefault;
  /** 队列最大运行应用数默认值 */
  private final int queueMaxAppsDefault;
  /** 是否使用DRF策略 */
  private final boolean drfUsed;
  /** 是否使用百分比方式转换容量 */
  private final boolean usePercentages;

  /** 转换选项，包含错误处理等配置 */
  private ConversionOptions conversionOptions;

  /**
   * 通过Builder构造FSQueueConverter实例。
   * @param builder 构建器对象
   */
  public FSQueueConverter(FSQueueConverterBuilder builder) {
    this.ruleHandler = builder.ruleHandler;
    this.capacitySchedulerConfig = builder.capacitySchedulerConfig;
    this.preemptionEnabled = builder.preemptionEnabled;
    this.sizeBasedWeight = builder.sizeBasedWeight;
    this.clusterResource = builder.clusterResource;
    this.queueMaxAMShareDefault = builder.queueMaxAMShareDefault;
    this.queueMaxAppsDefault = builder.queueMaxAppsDefault;
    this.conversionOptions = builder.conversionOptions;
    this.drfUsed = builder.drfUsed;
    this.usePercentages = builder.usePercentages;
  }

  /**
   * 递归转换整个队列层次结构。
   * @param queue 当前处理队列
   */
  public void convertQueueHierarchy(FSQueue queue) {
    List<FSQueue> children = queue.getChildQueues();
    final String queueName = queue.getName();

    emitChildQueues(queueName, children);
    emitMaxAMShare(queueName, queue);
    emitMaxParallelApps(queueName, queue);
    emitMaxAllocations(queueName, queue);
    emitPreemptionDisabled(queueName, queue);

    emitChildCapacity(queue);
    emitMaximumCapacity(queueName, queue);
    emitSizeBasedWeight(queueName);
    emitOrderingPolicy(queueName, queue);
    checkMaxChildCapacitySetting(queue);
    emitDefaultUserLimitFactor(queueName, children);

    for (FSQueue childQueue : children) {
      convertQueueHierarchy(childQueue);
    }
  }

  /**
   * 生成子队列列表配置：yarn.scheduler.capacity.&lt;queue-name&gt;.queues.
   * @param queueName 当前队列名称
   * @param children 子队列列表
   */
  private void emitChildQueues(String queueName, List<FSQueue> children) {
    ruleHandler.handleChildQueueCount(queueName, children.size());

    if (children.size() > 0) {
      List<String> childQueues = children.stream()
          .map(child -> getQueueShortName(child.getName()))
          .collect(Collectors.toList());
      capacitySchedulerConfig.setQueues(new QueuePath(queueName),
          childQueues.toArray(new String[0]));
    }
  }

  /**
   * 转换maxAMShare配置到maximum-am-resource-percent。
   * &lt;maxAMShare&gt; ==> yarn.scheduler.capacity.&lt;queue-name&gt;.maximum-am-resource-percent.
   * @param queueName 队列名称
   * @param queue 队列对象
   */
  private void emitMaxAMShare(String queueName, FSQueue queue) {
    float queueMaxAmShare = queue.getMaxAMShare();

    // Direct floating point comparison is OK here
    if (queueMaxAmShare != 0.0f
        && queueMaxAmShare != queueMaxAMShareDefault
        && queueMaxAmShare != QUEUE_MAX_AM_SHARE_DISABLED) {
      capacitySchedulerConfig.setMaximumApplicationMasterResourcePerQueuePercent(
          new QueuePath(queueName), queueMaxAmShare);
    }

    if (queueMaxAmShare == QUEUE_MAX_AM_SHARE_DISABLED
        && queueMaxAmShare != queueMaxAMShareDefault) {
      capacitySchedulerConfig.setMaximumApplicationMasterResourcePerQueuePercent(
          new QueuePath(queueName), 1.0f);
    }
  }

  /**
   * 转换maxRunningApps到max-parallel-apps配置。
   * &lt;maxRunningApps&gt; ==> yarn.scheduler.capacity.&lt;queue-name&gt;.max-parallel-apps.
   * @param queueName 队列名称
   * @param queue 队列对象
   */
  private void emitMaxParallelApps(String queueName, FSQueue queue) {
    if (queue.getMaxRunningApps() != MAX_RUNNING_APPS_UNSET
        && queue.getMaxRunningApps() != queueMaxAppsDefault) {
      capacitySchedulerConfig.setMaxParallelAppsForQueue(new QueuePath(queueName),
          String.valueOf(queue.getMaxRunningApps()));
    }
  }

  /**
   * 转换maxResources到maximum-capacity配置。
   * &lt;maxResources&gt; ==> yarn.scheduler.capacity.&lt;queue-name&gt;.maximum-capacity.
   * @param queueName 队列名称
   * @param queue 队列对象
   */
  private void emitMaximumCapacity(String queueName, FSQueue queue) {
    ConfigurableResource rawMaxShare = queue.getRawMaxShare();
    final Resource maxResource = rawMaxShare.getResource();

    if ((maxResource == null && rawMaxShare.getPercentages() != null)
        || isNotUnboundedResource(maxResource)) {
      ruleHandler.handleMaxResources();
    }

    capacitySchedulerConfig.setMaximumCapacity(new QueuePath(queueName),
        100.0f);
  }

  /**
   * 转换maxContainerAllocation到队列最大容器分配配置。
   * &lt;maxContainerAllocation&gt; ==> yarn.scheduler.capacity.&lt;queue-name&gt;.maximum-allocation-mb / vcores.
   * @param queueName 队列名称
   * @param queue 队列对象
   */
  private void emitMaxAllocations(String queueName, FSQueue queue) {
    Resource maxAllocation = queue.getMaximumContainerAllocation();

    if (isNotUnboundedResource(maxAllocation)) {
      int parentMaxVcores = Integer.MIN_VALUE;
      long parentMaxMemory = Integer.MIN_VALUE;

      if (queue.getParent() != null) {
        FSQueue parent = queue.getParent();
        Resource parentMaxAllocation = parent.getMaximumContainerAllocation();
        if (isNotUnboundedResource(parentMaxAllocation)) {
          parentMaxVcores = parentMaxAllocation.getVirtualCores();
          parentMaxMemory = parentMaxAllocation.getMemorySize();
        }
      }

      int maxVcores = maxAllocation.getVirtualCores();
      long maxMemory = maxAllocation.getMemorySize();

      // 仅当和父配置不同时才生成配置
      if (maxVcores != parentMaxVcores || maxMemory != parentMaxMemory) {
        capacitySchedulerConfig.setQueueMaximumAllocationMb(
            new QueuePath(queueName), (int) maxMemory);

        capacitySchedulerConfig.setQueueMaximumAllocationVcores(
            new QueuePath(queueName), maxVcores);
      }
    }
  }

  /**
   * 转换allowPreemptionFrom到disable_preemption配置。
   * &lt;allowPreemptionFrom&gt; ==> yarn.scheduler.capacity.&lt;queue-name&gt;.disable_preemption.
   * @param queueName 队列名称
   * @param queue 队列对象
   */
  private void emitPreemptionDisabled(String queueName, FSQueue queue) {
    if (preemptionEnabled && !queue.isPreemptable()) {
      capacitySchedulerConfig.setPreemptionDisabled(new QueuePath(queueName), true);
    }
  }

  /**
   * 为叶子队列设置默认userLimitFactor为-1（关闭用户限制）。
   * @param queueName 队列名称
   * @param children 子队列列表
   */
  public void emitDefaultUserLimitFactor(String queueName, List<FSQueue> children) {
    if (children.isEmpty() &&
            !capacitySchedulerConfig.isAutoQueueCreationV2Enabled(new QueuePath(queueName))) {
      capacitySchedulerConfig.setUserLimitFactor(new QueuePath(queueName), -1.0f);
    }
  }

  /**
   * 转换基于大小的权重配置到容量调度器对应配置。
   * yarn.scheduler.fair.sizebasedweight ==> yarn.scheduler.capacity.&lt;queue-path&gt;.ordering-policy.fair.enable-size-based-weight.
   * @param queueName 队列名称
   */
  private void emitSizeBasedWeight(String queueName) {
    if (sizeBasedWeight) {
      capacitySchedulerConfig.setBoolean(PREFIX + queueName +
          ".ordering-policy.fair.enable-size-based-weight", true);
    }
  }

  /**
   * 转换调度策略到容量调度器排序策略配置。
   * &lt;schedulingPolicy&gt; ==> yarn.scheduler.capacity.&lt;queue-path&gt;.ordering-policy.
   * @param queueName 队列名称
   * @param queue 队列对象
   */
  private void emitOrderingPolicy(String queueName, FSQueue queue) {
    if (queue instanceof FSLeafQueue) {
      String policy = queue.getPolicy().getName();

      switch (policy) {
      case DominantResourceFairnessPolicy.NAME:
        capacitySchedulerConfig.setOrderingPolicy(new QueuePath(queueName), FAIR_POLICY);
        break;
      case FairSharePolicy.NAME:
        capacitySchedulerConfig.setOrderingPolicy(new QueuePath(queueName), FAIR_POLICY);
        if (drfUsed) {
          ruleHandler.handleFairAsDrf(queueName);
        }
        break;
      case FifoPolicy.NAME:
        capacitySchedulerConfig.setOrderingPolicy(new QueuePath(queueName), FIFO_POLICY);
        break;
      default:
        String msg = String.format("Unexpected ordering policy " +
            "on queue %s: %s", queue, policy);
        conversionOptions.handleConversionError(msg);
      }
    }
  }

  /**
   * 将权重和最小资源转换为容量调度器容量配置。
   * weight + minResources ==> yarn.scheduler.capacity.&lt;queue-name&gt;.capacity.
   * @param queue 当前队列
   */
  private void emitChildCapacity(FSQueue queue) {
    CapacityConverter converter =
        CapacityConverterFactory.getConverter(usePercentages);

    converter.convertWeightsForChildQueues(queue,
        capacitySchedulerConfig);

    if (Resources.none().compareTo(queue.getMinShare()) != 0) {
      ruleHandler.handleMinResources();
    }
  }

  /**
   * 检查子队列最大资源配置，该配置不被容量调度器支持，输出警告。
   * 容量调度器leaf-queue-template.capacity仅接受单个百分比值，不支持maxChildQueueResource。
   * @param queue 当前队列
   */
  private void checkMaxChildCapacitySetting(FSQueue queue) {
    if (queue.getMaxChildQueueResource() != null) {
      Resource resource = queue.getMaxChildQueueResource().getResource();

      if ((resource != null && isNotUnboundedResource(resource))
          || queue.getMaxChildQueueResource().getPercentages() != null) {
        // 定义了最大子资源，容量调度器不支持该特性，通知处理器
        ruleHandler.handleMaxChildCapacity();
      }
    }
  }

  /**
   * 从全路径队列名提取短名称（最后一个点后的部分）。
   * @param queueName 全路径队列名
   * @return 队列短名称
   */
  private String getQueueShortName(String queueName) {
    int lastDot = queueName.lastIndexOf(".");
    return queueName.substring(lastDot + 1);
  }

  /**
   * 判断资源是否不是无限资源。
   * @param res 待判断资源对象
   * @return true表示不是无限资源，false表示是无限资源
   */
  private boolean isNotUnboundedResource(Resource res) {
    return Resources.unbounded().compareTo(res) != 0;
  }
}