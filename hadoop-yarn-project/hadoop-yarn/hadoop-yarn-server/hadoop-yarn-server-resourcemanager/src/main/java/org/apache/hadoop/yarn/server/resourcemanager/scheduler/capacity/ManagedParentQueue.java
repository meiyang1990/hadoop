// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica
    .FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getQueueCapacityConfigParser;

/**
 * 支持自动创建子队列的父队列。该队列初始不包含任何子队列，所有叶子子队列均会自动创建。
 * 当前不允许预先配置的叶子/父队列与自动创建叶子队列共存，自动创建当前仅支持叶子队列。
 */
public class ManagedParentQueue extends AbstractManagedParentQueue {

  // 当保证容量超出限制时，是否禁止自动创建队列
  private boolean shouldFailAutoCreationWhenGuaranteedCapacityExceeded = false;

  private static final Logger LOG = LoggerFactory.getLogger(
      ManagedParentQueue.class);

  /**
   * 构造函数，创建支持自动创建子队列的父队列。
   * @param queueContext 容量调度器队列上下文
   * @param queueName 队列名称
   * @param parent 父队列
   * @param old 旧队列对象（用于重新初始化）
   * @throws IOException 初始化失败时抛出IO异常
   */
  public ManagedParentQueue(final CapacitySchedulerQueueContext queueContext,
      final String queueName, final CSQueue parent, final CSQueue old)
      throws IOException {
    super(queueContext, queueName, parent, old);
    super.setupQueueConfigs(queueContext.getClusterResource());
    shouldFailAutoCreationWhenGuaranteedCapacityExceeded =
        queueContext.getConfiguration()
            .getShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(
                getQueuePathObject());

    leafQueueTemplate = initializeLeafQueueConfigs().build();

    initializeQueueManagementPolicy();
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {

    writeLock.lock();
    try {
      // 验证新队列合法性
      validate(newlyParsedQueue);

      // 重新加载配置参数
      shouldFailAutoCreationWhenGuaranteedCapacityExceeded =
          queueContext.getConfiguration()
              .getShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(
                  getQueuePathObject());

      // 如果开启了容量超限检查，验证子队列总容量不超过父队列保证容量
      if (shouldFailAutoCreationWhenGuaranteedCapacityExceeded) {
        float childCap = sumOfChildCapacities();
        if (getCapacity() < childCap) {
          throw new IOException(
              "Total of Auto Created leaf queues guaranteed capacity : "
                  + childCap + " exceeds Parent queue's " + getQueuePath()
                  + " guaranteed capacity " + getCapacity() + ""
                  + ".Cannot enforce policy to auto"
                  + " create queues beyond parent queue's capacity");
        }
      }

      // 重新初始化叶子队列模板配置
      leafQueueTemplate = initializeLeafQueueConfigs().build();

      // 调用父类重新初始化
      super.reinitialize(newlyParsedQueue, clusterResource);

      // 触发所有已有子队列重新初始化，重新计算绝对容量上限
      for (CSQueue res : this.getChildQueues()) {
        res.reinitialize(res, clusterResource);
      }

      // 清空队列管理策略状态
      reinitializeQueueManagementPolicy();

      // 根据策略重新计算队列管理变更
      final List<QueueManagementChange> queueManagementChanges =
          queueManagementPolicy.computeQueueManagementChanges();

      // 验证并应用队列管理变更
      validateAndApplyQueueManagementChanges(queueManagementChanges);

      LOG.info(
          "Reinitialized Managed Parent Queue: [{}] with capacity [{}]"
              + " with max capacity [{}]",
          getQueueName(), super.getCapacity(), super.getMaximumCapacity());
    } catch (YarnException ye) {
      LOG.error("Exception while computing policy changes for leaf queue : "
          + getQueuePath(), ye);
      throw new IOException(ye);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 初始化队列自动管理策略。
   * @throws IOException 初始化失败时抛出IO异常
   */
  private void initializeQueueManagementPolicy() throws IOException {
    queueManagementPolicy =
        queueContext.getConfiguration().getAutoCreatedQueueManagementPolicyClass(
            getQueuePathObject());

    queueManagementPolicy.init(this);
  }

  /**
   * 重新初始化队列自动管理策略，策略类变更则重新创建实例，否则重置现有实例状态。
   * @throws IOException 初始化失败时抛出IO异常
   */
  private void reinitializeQueueManagementPolicy() throws IOException {
    AutoCreatedQueueManagementPolicy managementPolicy =
        queueContext.getConfiguration().getAutoCreatedQueueManagementPolicyClass(
            getQueuePathObject());

    if (!(managementPolicy.getClass().equals(
        this.queueManagementPolicy.getClass()))) {
      queueManagementPolicy = managementPolicy;
      queueManagementPolicy.init(this);
    } else{
      queueManagementPolicy.reinitialize(this);
    }
  }

  /**
   * 初始化自动创建叶子队列的配置模板。
   * @return 配置模板构建器
   * @throws IOException 初始化失败时抛出IO异常
   */
  protected AutoCreatedLeafQueueConfig.Builder initializeLeafQueueConfigs() throws IOException {

    AutoCreatedLeafQueueConfig.Builder builder =
        new AutoCreatedLeafQueueConfig.Builder();

    CapacitySchedulerConfiguration configuration =
        queueContext.getConfiguration();

    // TODO load configs into CapacitySchedulerConfiguration instead of duplicating them
    // 获取叶子队列模板配置前缀
    String leafQueueTemplateConfPrefix = getLeafQueueConfigPrefix();
    // 将模板配置加载到调度器配置
    CapacitySchedulerConfiguration autoCreatedTemplateConfig =
        super.initializeLeafQueueConfigs(leafQueueTemplateConfPrefix);
    builder.configuration(autoCreatedTemplateConfig);
    // 初始化资源配额对象
    QueueResourceQuotas queueResourceQuotas = new QueueResourceQuotas();
    // 设置绝对资源模板
    setAbsoluteResourceTemplates(configuration, queueResourceQuotas);

    QueuePath templateQueuePath = QueuePrefixes
        .getAutoCreatedQueueObjectTemplateConfPrefix(getQueuePathObject());
    Set<String> templateConfiguredNodeLabels = queueContext
        .getQueueManager().getConfiguredNodeLabelsForAllQueues()
        .getLabelsByQueue(templateQueuePath.getFullPath());
    // 加载模板容量配置
    QueueCapacities queueCapacities = new QueueCapacities(false);
    CSQueueUtils.loadCapacitiesByLabelsFromConf(templateQueuePath,
        queueCapacities,
        configuration,
        templateConfiguredNodeLabels);

    /**
     * Populate leaf queue template (of Parent resources configured in
     * ABSOLUTE_RESOURCE) capacities with actual values for which configured has
     * been defined in ABSOLUTE_RESOURCE format.
     *
     */
    // 如果当前父队列使用绝对资源容量配置，更新叶子队列容量计算
    if (this.capacityConfigType.equals(CapacityConfigType.ABSOLUTE_RESOURCE)) {
      updateQueueCapacities(queueCapacities);
    }
    builder.capacities(queueCapacities);
    builder.resourceQuotas(queueResourceQuotas);
    return builder;
  }

  /**
   * 设置自动创建叶子队列的绝对资源配额模板。
   * @param configuration 调度器配置
   * @param queueResourceQuotas 资源配额对象
   * @throws IOException 配置类型不匹配时抛出IO异常
   */
  private void setAbsoluteResourceTemplates(CapacitySchedulerConfiguration configuration,
                                            QueueResourceQuotas queueResourceQuotas) throws IOException {
    QueuePath templateQueuePath = QueuePrefixes
        .getAutoCreatedQueueObjectTemplateConfPrefix(getQueuePathObject());
    Set<String> templateConfiguredNodeLabels = queueContext
        .getQueueManager().getConfiguredNodeLabelsForAllQueues()
        .getLabelsByQueue(templateQueuePath.getFullPath());

    // 遍历每个节点标签，读取最小/最大资源配置
    for (String nodeLabel : templateConfiguredNodeLabels) {
      Resource templateMinResource = configuration.getMinimumResourceRequirement(
          nodeLabel, templateQueuePath, resourceTypes);
      queueResourceQuotas.setConfiguredMinResource(nodeLabel, templateMinResource);

      // 校验配置类型一致性：父队列是百分比配置，但叶子模板使用绝对资源，抛出异常
      if (this.capacityConfigType.equals(CapacityConfigType.PERCENTAGE)
          && !templateMinResource.equals(Resources.none())) {
        throw new IOException("Managed Parent Queue " + this.getQueuePath()
            + " config type is different from leaf queue template config type");
      }
    }
  }

  /**
   * 基于绝对资源配置更新叶子队列模板容量值，计算相对于父队列的比例。
   * @param queueCapacities 叶子队列容量对象
   */
  private void updateQueueCapacities(QueueCapacities queueCapacities) {
    CapacitySchedulerConfiguration configuration =
        queueContext.getConfiguration();

    // 遍历每个节点标签更新容量
    for (String label : queueCapacities.getExistingNodeLabels()) {
      // 计算叶子队列保证容量占父队列保证容量的比例
      queueCapacities.setCapacity(label,
          resourceCalculator.divide(
              queueContext.getClusterResource(),
              configuration.getMinimumResourceRequirement(
                  label,
                  QueuePrefixes
                      .getAutoCreatedQueueObjectTemplateConfPrefix(getQueuePathObject()),
                  resourceTypes),
              getQueueResourceQuotas().getConfiguredMinResource(label)));

      // 获取叶子队列和父队列最大资源，取较小值作为叶子队列有效最大资源
      Resource childMaxResource = configuration
          .getMaximumResourceRequirement(label,
              QueuePrefixes
                  .getAutoCreatedQueueObjectTemplateConfPrefix(getQueuePathObject()),
              resourceTypes);
      Resource parentMaxRes = getQueueResourceQuotas()
          .getConfiguredMaxResource(label);

      Resource effMaxResource = Resources.min(
          resourceCalculator,
          queueContext.getClusterResource(),
          childMaxResource.equals(Resources.none()) ? parentMaxRes
              : childMaxResource,
          parentMaxRes);

      // 计算叶子队列最大容量占父队列最大容量的比例
      queueCapacities.setMaximumCapacity(
          label, resourceCalculator.divide(
              queueContext.getClusterResource(),
               effMaxResource,
               getQueueResourceQuotas().getConfiguredMaxResource(label)));

      // 计算绝对容量 = 叶子比例 × 父队列绝对容量
      queueCapacities.setAbsoluteCapacity(
          label, queueCapacities.getCapacity(label)
          * getQueueCapacities().getAbsoluteCapacity(label));

      // 计算绝对最大容量 = 叶子比例 × 父队列绝对最大容量
      queueCapacities.setAbsoluteMaximumCapacity(label,
          queueCapacities.getMaximumCapacity(label)
          * getQueueCapacities().getAbsoluteMaximumCapacity(label));
    }
  }

  /**
   * 验证重新初始化传入的新队列合法性。
   * @param newlyParsedQueue 新解析的队列对象
   * @throws IOException 验证不通过时抛出IO异常
   */
  protected void validate(final CSQueue newlyParsedQueue) throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof ManagedParentQueue) || !newlyParsedQueue
        .getQueuePath().equals(getQueuePath())) {
      throw new IOException(
          "Trying to reinitialize " + getQueuePath() + " from "
              + newlyParsedQueue.getQueuePath());
    }
  }

  @Override
  public void addChildQueue(CSQueue childQueue)
      throws SchedulerDynamicEditException, IOException {

    writeLock.lock();
    try {
      // 验证子队列类型必须是自动创建叶子队列
      if (childQueue == null || !(childQueue instanceof AutoCreatedLeafQueue)) {
        throw new SchedulerDynamicEditException(
            "Expected child queue to be an instance of AutoCreatedLeafQueue");
      }

      CapacitySchedulerConfiguration conf = queueContext.getConfiguration();
      ManagedParentQueue parentQueue =
          (ManagedParentQueue) childQueue.getParent();

      // 验证父队列不为空
      if (parentQueue == null) {
        throw new SchedulerDynamicEditException(
            "Parent Queue is null, should not add child queue!");
      }

      String leafQueuePath = childQueue.getQueuePath();
      // 获取父队列允许的最大子队列数量限制
      int maxQueues = conf.getAutoCreatedQueuesMaxChildQueuesLimit(
          parentQueue.getQueuePathObject());

      // 检查子队列数量是否超出限制
      if (parentQueue.getChildQueues().size() >= maxQueues) {
        throw new SchedulerDynamicEditException(
            "Cannot auto create leaf queue " + leafQueuePath + ".Max Child "
                + "Queue limit exceeded which is configured as : " + maxQueues
                + " and number of child queues is : " + parentQueue
                .getChildQueues().size());
      }

      // 如果开启保证容量超限检查，验证新增子队列后总保证容量不超过父队列
      if (shouldFailAutoCreationWhenGuaranteedCapacityExceeded) {
        if (getLeafQueueTemplate().getQueueCapacities().getAbsoluteCapacity()
            + parentQueue.sumOfChildAbsCapacities() > parentQueue
            .getAbsoluteCapacity()) {
          throw new SchedulerDynamicEditException(
              "Cannot auto create leaf queue " + leafQueuePath + ". Child "
                  + "queues capacities have reached parent queue : "
                  + parentQueue.getQueuePath() + "'s guaranteed capacity");
        }
      }

      // 更新策略中模板绝对容量
      ((GuaranteedOrZeroCapacityOverTimePolicy) queueManagementPolicy)
          .updateTemplateAbsoluteCapacities(parentQueue.getQueueCapacities());

      // 调用父类添加子队列
      AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) childQueue;
      super.addChildQueue(leafQueue);

      /* Below is to avoid Setting Queue Capacity to NaN when ClusterResource
         is zero during RM Startup with DominantResourceCalculator */
      // 绝对资源配置模式下重新更新容量，避免RM启动时集群资源为0导致容量为NaN
      if (this.capacityConfigType.equals(
          CapacityConfigType.ABSOLUTE_RESOURCE)) {
        QueueCapacities queueCapacities =
            getLeafQueueTemplate().getQueueCapacities();
        updateQueueCapacities(queueCapacities);
      }

      // 设置叶子队列容量向量
      setLeafQueuesCapacityVector(leafQueue);

      // 从策略获取叶子队列初始配置，根据模板重新初始化叶子队列
      final AutoCreatedLeafQueueConfig initialLeafQueueTemplate =
          queueManagementPolicy.getInitialLeafQueueConfiguration(leafQueue);
      leafQueue.reinitializeFromTemplate(initialLeafQueueTemplate);

      // 更新集群资源，触发所有绝对资源和有效资源重新计算
      updateClusterResource(queueContext.getClusterResource(),
          new ResourceLimits(queueContext.getClusterResource()));
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 从模板解析并设置叶子队列的容量向量（按节点标签）。
   * @param leafQueue 目标叶子队列
   */
  private void setLeafQueuesCapacityVector(AutoCreatedLeafQueue leafQueue) {
    // Parse the capacityVector specified in the leaf-template
    CapacitySchedulerConfiguration leafConfig = leafQueueTemplate.getLeafQueueConfigs();
    Set<String> templateConfiguredNodeLabels = queueContext
        .getQueueManager().getConfiguredNodeLabelsForAllQueues()
        .getLabelsByQueue(queuePath.getFullPath());
    for (String label : templateConfiguredNodeLabels) {
      final String leafConfigPath =
          QueuePrefixes.getNodeLabelPrefix(
              QueuePrefixes.getAutoCreatedQueueObjectTemplateConfPrefix(getQueuePathObject()),
                  label);
      String capacityString = leafConfig.get(