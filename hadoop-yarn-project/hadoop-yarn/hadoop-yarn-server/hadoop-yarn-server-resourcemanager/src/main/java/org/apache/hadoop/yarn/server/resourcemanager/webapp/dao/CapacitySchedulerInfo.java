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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.helper.CapacitySchedulerInfoHelper;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo.getSortedQueueAclInfoList;

/**
 * 容量调度器信息数据访问对象，为Web UI提供容量调度器队列信息
 * 封装根队列/父队列的容量、权重、子队列等调度信息，用于REST接口返回
 */
@XmlRootElement(name = "capacityScheduler")
@XmlType(name = "capacityScheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerInfo extends SchedulerInfo {

  // 队列配置容量（百分比）
  protected float capacity;
  // 队列已使用容量（百分比）
  protected float usedCapacity;
  // 队列最大容量（百分比）
  protected float maxCapacity;
  // 队列权重
  protected float weight;
  // 队列归一化权重
  protected float normalizedWeight;
  // 队列多维容量向量信息
  protected QueueCapacityVectorInfo queueCapacityVectorInfo;
  // 队列名称
  protected String queueName;
  // 队列完整路径
  private String queuePath;
  // 队列最大并行应用数
  protected int maxParallelApps;
  // 是否使用绝对资源配置模式
  private boolean isAbsoluteResource;
  // 子队列信息列表
  protected CapacitySchedulerQueueInfoList queues;
  // 队列各类容量统计信息
  protected QueueCapacitiesInfo capacities;
  // 容量调度器健康状态信息
  protected CapacitySchedulerHealthInfo health;
  // 容器最大分配资源
  protected ResourceInfo maximumAllocation;
  // 队列访问控制列表信息
  protected QueueAclsInfo queueAcls;
  // 队列优先级
  protected int queuePriority;
  // 队列排序策略名称
  protected String orderingPolicyInfo;
  // 队列调度模式
  protected String mode;
  // 队列类型
  protected String queueType;
  // 队列创建方式
  protected String creationMethod;
  // 是否符合自动创建条件
  protected String autoCreationEligibility;
  // 默认节点标签表达式
  protected String defaultNodeLabelExpression;
  // 自动创建队列模板属性
  protected AutoQueueTemplatePropertiesInfo autoQueueTemplateProperties;
  // 自动创建队列父队列专属模板属性
  protected AutoQueueTemplatePropertiesInfo autoQueueParentTemplateProperties;
  // 自动创建队列叶子队列专属模板属性
  protected AutoQueueTemplatePropertiesInfo autoQueueLeafTemplateProperties;

  @XmlTransient
  static final float EPSILON = 1e-8f;

  public CapacitySchedulerInfo() {
  } // JAXB needs this

  /**
   * 根据容量调度器队列对象构造容量调度器信息
   * @param parent 当前队列（父队列/根队列）
   * @param cs 容量调度器实例
   */
  public CapacitySchedulerInfo(CSQueue parent, CapacityScheduler cs) {
    this.queueName = parent.getQueueName();
    this.queuePath = parent.getQueuePath();
    this.usedCapacity = parent.getUsedCapacity() * 100.0F;
    this.capacity = parent.getCapacity() * 100;
    this.queueCapacityVectorInfo = new QueueCapacityVectorInfo(
            parent.getConfiguredCapacityVector(NO_LABEL));
    float max = parent.getMaximumCapacity();
    // 修正异常最大容量值
    if (max < EPSILON || max > 1f)
      max = 1f;
    this.maxCapacity = max * 100;
    this.weight = parent.getQueueCapacities().getWeight();
    this.normalizedWeight = parent.getQueueCapacities().getNormalizedWeight();
    this.maxParallelApps = parent.getMaxParallelApps();

    capacities = new QueueCapacitiesInfo(parent, false);
    queues = getQueues(cs, parent);
    health = new CapacitySchedulerHealthInfo(cs);
    maximumAllocation = new ResourceInfo(parent.getMaximumAllocation());

    isAbsoluteResource = parent.getCapacityConfigType() ==
        AbstractCSQueue.CapacityConfigType.ABSOLUTE_RESOURCE;

    CapacitySchedulerConfiguration conf = cs.getConfiguration();
    queueAcls = new QueueAclsInfo();
    queueAcls.addAll(getSortedQueueAclInfoList(parent, new QueuePath(queuePath), conf));

    queuePriority = parent.getPriority().getPriority();
    // 如果是父队列，额外获取父队列特有属性
    if (parent instanceof AbstractParentQueue) {
      AbstractParentQueue queue = (AbstractParentQueue) parent;
      orderingPolicyInfo = queue.getQueueOrderingPolicy()
          .getConfigName();
      autoQueueTemplateProperties = CapacitySchedulerInfoHelper
          .getAutoCreatedTemplate(queue.getAutoCreatedQueueTemplate()
              .getTemplateProperties());
      autoQueueParentTemplateProperties = CapacitySchedulerInfoHelper
          .getAutoCreatedTemplate(queue.getAutoCreatedQueueTemplate()
              .getParentOnlyProperties());
      autoQueueLeafTemplateProperties = CapacitySchedulerInfoHelper
          .getAutoCreatedTemplate(queue.getAutoCreatedQueueTemplate()
              .getLeafOnlyProperties());
    }
    mode = CapacitySchedulerInfoHelper.getMode(parent);
    queueType = CapacitySchedulerInfoHelper.getQueueType(parent);
    creationMethod = CapacitySchedulerInfoHelper.getCreationMethod(parent);
    autoCreationEligibility = CapacitySchedulerInfoHelper
        .getAutoCreationEligibility(parent);

    defaultNodeLabelExpression = parent.getDefaultNodeLabelExpression();
    schedulerName = "Capacity Scheduler";
  }

  public float getCapacity() {
    return this.capacity;
  }

  public float getUsedCapacity() {
    return this.usedCapacity;
  }

  public QueueCapacitiesInfo getCapacities() {
    return capacities;
  }

  public float getMaxCapacity() {
    return this.maxCapacity;
  }

  public String getQueueName() {
    return this.queueName;
  }

  public String getQueuePath() {
    return this.queuePath;
  }

  public ResourceInfo getMaximumAllocation() {
    return maximumAllocation;
  }

  public QueueAclsInfo getQueueAcls() {
    return queueAcls;
  }

  public int getPriority() {
    return queuePriority;
  }

  public String getOrderingPolicyInfo() {
    return orderingPolicyInfo;
  }

  public CapacitySchedulerQueueInfoList getQueues() {
    return this.queues;
  }

  /**
   * 递归收集当前队列的所有子队列信息，先叶子队列后父队列解决JAXB序列化问题
   * @param cs 容量调度器实例
   * @param parent 当前父队列
   * @return 排序后的子队列信息列表
   */
  protected CapacitySchedulerQueueInfoList getQueues(
      CapacityScheduler cs, CSQueue parent) {
    CapacitySchedulerQueueInfoList queuesInfo =
        new CapacitySchedulerQueueInfoList();
    // JAXB marshalling leads to situation where the "type" field injected
    // for JSON changes from string to array depending on order of printing
    // Issue gets fixed if all the leaf queues are marshalled before the
    // non-leaf queues. See YARN-4785 for more details.
    List<CSQueue> childQueues = new ArrayList<>();
    List<CSQueue> childLeafQueues = new ArrayList<>();
    List<CSQueue> childNonLeafQueues = new ArrayList<>();
    // 按叶子队列/非叶子队列分类
    for (CSQueue queue : parent.getChildQueues()) {
      if (queue instanceof AbstractLeafQueue) {
        childLeafQueues.add(queue);
      } else {
        childNonLeafQueues.add(queue);
      }
    }
    // 先放叶子队列，再放非叶子队列
    childQueues.addAll(childLeafQueues);
    childQueues.addAll(childNonLeafQueues);

    // 递归构造每个子队列信息
    for (CSQueue queue : childQueues) {
      CapacitySchedulerQueueInfo info;
      if (queue instanceof AbstractLeafQueue) {
        info = new CapacitySchedulerLeafQueueInfo(cs, (AbstractLeafQueue) queue);
      } else {
        info = new CapacitySchedulerQueueInfo(cs, queue);
        info.queues = getQueues(cs, queue);
      }
      queuesInfo.addToQueueInfoList(info);
    }
    return queuesInfo;
  }

  public String getMode() {
    return mode;
  }

  public String getQueueType() {
    return queueType;
  }
}