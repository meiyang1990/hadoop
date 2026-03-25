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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;

import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType.ABSOLUTE_RESOURCE;

/**
 * 自动创建的叶子队列，由AbstractManagedParentQueue的底层实现自动生成。
 * 例如：预约队列的PlanQueue，或动态队列场景下的ManagedParentQueue自动创建的队列
 */
public class AutoCreatedLeafQueue extends AbstractAutoCreatedLeafQueue {
  private static final Logger LOG = LoggerFactory
      .getLogger(AutoCreatedLeafQueue.class);

  /**
   * 构造自动创建的叶子队列，初始化后将容量设为0
   * @param queueContext 容量调度器队列上下文
   * @param queueName 队列名称
   * @param parent 父队列（必须是ManagedParentQueue类型）
   * @throws IOException 初始化失败时抛出异常
   */
  public AutoCreatedLeafQueue(CapacitySchedulerQueueContext queueContext, String queueName,
      ManagedParentQueue parent) throws IOException {
    super(queueContext, queueName, parent, null);
    parent.setLeafQueueConfigs(queueName);
    super.setupQueueConfigs(queueContext.getClusterResource());

    updateCapacitiesToZero();
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {
    // 获取写锁保证并发安全
    writeLock.lock();
    try {
      // 验证待重新初始化的队列合法性
      validate(newlyParsedQueue);

      ManagedParentQueue managedParentQueue = (ManagedParentQueue) parent;
      // 更新父队列中的叶子队列配置
      managedParentQueue.setLeafQueueConfigs(newlyParsedQueue.getQueueShortName());
      // 调用父类重新初始化逻辑
      super.reinitialize(newlyParsedQueue, clusterResource);

      // 重新初始化后重置容量为0，避免父队列资源超配
      updateCapacitiesToZero();

    } finally {
      // 释放写锁
      writeLock.unlock();
    }
  }

  /**
   * 从队列模板重新初始化当前叶子队列的容量配置
   * @param leafQueueTemplate 叶子队列配置模板
   */
  public void reinitializeFromTemplate(AutoCreatedLeafQueueConfig leafQueueTemplate) {
    // 获取写锁保证并发安全
    writeLock.lock();
    try {
      // 从模板中获取容量配置
      QueueCapacities capacities = leafQueueTemplate.getQueueCapacities();

      // 合并模板中的容量和配额配置到当前队列
      mergeCapacities(capacities, leafQueueTemplate.getResourceQuotas());

    } finally {
      // 释放写锁
      writeLock.unlock();
    }
  }

  /**
   * 将模板中的容量和配额合并到当前队列，更新有效资源
   * @param capacities 模板容量配置
   * @param resourceQuotas 模板资源配额
   */
  public void mergeCapacities(QueueCapacities capacities, QueueResourceQuotas resourceQuotas) {
    // 遍历所有已存在的节点标签
    for ( String nodeLabel : capacities.getExistingNodeLabels()) {
      // 更新各容量指标
      queueCapacities.setCapacity(nodeLabel,
          capacities.getCapacity(nodeLabel));
      queueCapacities.setAbsoluteCapacity(nodeLabel, capacities
          .getAbsoluteCapacity(nodeLabel));
      queueCapacities.setMaximumCapacity(nodeLabel, capacities
          .getMaximumCapacity(nodeLabel));
      queueCapacities.setAbsoluteMaximumCapacity(nodeLabel, capacities
          .getAbsoluteMaximumCapacity(nodeLabel));

      // 获取当前标签对应的集群总资源
      Resource resourceByLabel = labelManager.getResourceByLabel(nodeLabel,
          queueContext.getClusterResource());
      // 处理有效最小资源：绝对资源模式从模板取有效资源解决舍入误差，否则按百分比计算
      if (getCapacityConfigType().equals(ABSOLUTE_RESOURCE)
          && queueCapacities.getAbsoluteCapacity(nodeLabel) > 0) {
        getQueueResourceQuotas().setEffectiveMinResource(nodeLabel,
            resourceQuotas.getConfiguredMinResource(nodeLabel));
      } else {
        getQueueResourceQuotas().setEffectiveMinResource(nodeLabel,
            Resources.multiply(resourceByLabel,
                queueCapacities.getAbsoluteCapacity(nodeLabel)));
      }

      // 按百分比计算有效最大资源
      getQueueResourceQuotas().setEffectiveMaxResource(nodeLabel,
          Resources.multiply(resourceByLabel, queueCapacities
              .getAbsoluteMaximumCapacity(nodeLabel)));
    }
  }

  /**
   * 验证自动创建叶子队列模板配置合法性
   * @param template 待验证的模板配置
   * @throws SchedulerDynamicEditException 配置不合法时抛出异常
   */
  public void validateConfigurations(AutoCreatedLeafQueueConfig template)
      throws SchedulerDynamicEditException {
    QueueCapacities capacities = template.getQueueCapacities();
    for (String label : capacities.getExistingNodeLabels()) {
      float capacity = capacities.getCapacity(label);
      if (capacity < 0 || capacity > 1.0f) {
        throw new SchedulerDynamicEditException(
            "Capacity demand is not in the [0,1] range: " + capacity);
      }
    }
  }

  @Override
  protected void parseAndSetDynamicTemplates() {
    // 构造父队列模板的路径
    String parentTemplate = String.format("%s.%s", getParent().getQueuePath(),
        CapacitySchedulerConfiguration
            .AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX);
    // 获取父模板配置的节点标签
    Set<String> parentNodeLabels = queueContext
        .getQueueManager().getConfiguredNodeLabelsForAllQueues()
        .getLabelsByQueue(parentTemplate);

    // 如果父模板配置了多个节点标签，继承给当前队列
    if (parentNodeLabels != null && parentNodeLabels.size() > 1) {
      queueContext.getQueueManager().getConfiguredNodeLabelsForAllQueues()
          .setLabelsByQueue(getQueuePath(),
              new HashSet<>(parentNodeLabels));
    }
  }

  /**
   * 验证重新初始化传入的队列参数合法性
   * @param newlyParsedQueue 待重新初始化的队列
   * @throws IOException 参数不合法时抛出异常
   */
  private void validate(final CSQueue newlyParsedQueue) throws IOException {
    if (!(newlyParsedQueue instanceof AutoCreatedLeafQueue) || !newlyParsedQueue
        .getQueuePath().equals(getQueuePath())) {
      throw new IOException(
          "Error trying to reinitialize " + getQueuePath() + " from "
              + newlyParsedQueue.getQueuePath());
    }
  }

  /**
   * 将当前队列所有节点标签的容量设置为0，避免资源超配
   * 最大容量继承自父队列模板配置
   * @throws IOException 设置权限失败时抛出异常
   */
  private void updateCapacitiesToZero() throws IOException {
    try {
      for( String nodeLabel : parent.getQueueCapacities().getExistingNodeLabels
          ()) {
        setEntitlement(nodeLabel, new QueueEntitlement(0.0f,
            parent.getLeafQueueTemplate()
                .getQueueCapacities()
                .getMaximumCapacity(nodeLabel)));
      }
    } catch (SchedulerDynamicEditException e) {
      throw new IOException(e);
    }
  }
}