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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueStateManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.AppPriorityACLsManager;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getACLsForFlexibleAutoCreatedLeafQueue;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getACLsForFlexibleAutoCreatedParentQueue;

/**
 * 容量调度器队列管理器，负责管理容量调度器中所有队列的生命周期、层次结构和动态变更。
 * 核心职责包括：队列初始化解析、配置重载更新、动态队列创建删除、队列ACL权限设置等。
 */
@Private
@Unstable
public class CapacitySchedulerQueueManager implements SchedulerQueueManager<
    CSQueue, CapacitySchedulerConfiguration>{

  private static final Logger LOG = LoggerFactory.getLogger(
      CapacitySchedulerQueueManager.class);

  /**
   * 队列钩子，用于队列创建后自定义处理（测试扩展用）。
   */
  static class QueueHook {
    public CSQueue hook(CSQueue queue) {
      return queue;
    }
  }

  private static final QueueHook NOOP = new QueueHook();
  private CapacitySchedulerContext csContext;
  private final YarnAuthorizationProvider authorizer;
  private final CSQueueStore queues = new CSQueueStore();
  private CSQueue root;
  private final RMNodeLabelsManager labelManager;
  private AppPriorityACLsManager appPriorityACLManager;
  private CapacitySchedulerQueueCapacityHandler queueCapacityHandler;

  private QueueStateManager<CSQueue, CapacitySchedulerConfiguration>
      queueStateManager;
  private ConfiguredNodeLabels configuredNodeLabels;

  /**
   * 构造容量调度器队列管理器。
   * @param conf 配置对象
   * @param labelManager 节点标签管理器
   * @param appPriorityACLManager 应用优先级ACL管理器
   */
  public CapacitySchedulerQueueManager(Configuration conf,
      RMNodeLabelsManager labelManager,
      AppPriorityACLsManager appPriorityACLManager) {
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
    this.labelManager = labelManager;
    this.queueStateManager = new QueueStateManager<>();
    this.appPriorityACLManager = appPriorityACLManager;
    this.configuredNodeLabels = new ConfiguredNodeLabels();
    this.queueCapacityHandler = new CapacitySchedulerQueueCapacityHandler(labelManager,
        new CapacitySchedulerConfiguration(conf));
  }

  @Override
  public CSQueue getRootQueue() {
    return this.root;
  }

  @VisibleForTesting
  protected void setRootQueue(CSQueue rootQueue) {
    this.root = rootQueue;
  }

  @Override
  public Map<String, CSQueue> getQueues() {
    return queues.getFullNameQueues();
  }

  @VisibleForTesting
  public Map<String, CSQueue> getShortNameQueues() {
    return queues.getShortNameQueues();
  }

  @Override
  public void removeQueue(String queueName) {
    this.queues.remove(queueName);
  }

  @Override
  public void addQueue(String queueName, CSQueue queue) {
    this.queues.add(queue);
  }

  @Override
  public CSQueue getQueue(String queueName) {
    return queues.get(queueName);
  }

  public CSQueue getQueueByFullName(String name) {
    return queues.getByFullName(name);
  }

  /**
   * 规范化队列名称，将短名称转换为完整路径。
   * @param name 输入队列名称（可能是短名称）
   * @return 完整队列路径，找不到则返回原名称保证后续流程正常报错
   */
  String normalizeQueueName(String name) {
    CSQueue queue = this.queues.get(name);
    if (queue != null) {
      return queue.getQueuePath();
    }
    //We return the original name here instead of null, to make sure we don't
    // introduce a NPE, and let the process fail where it would fail for unknown
    // queues, resulting more informative error messages.
    return name;
  }

  public boolean isAmbiguous(String shortName) {
    return queues.isAmbiguous(shortName);
  }

  /**
   * 设置容量调度器上下文。
   * @param capacitySchedulerContext 容量调度器上下文
   */
  public void setCapacitySchedulerContext(
      CapacitySchedulerContext capacitySchedulerContext) {
    this.csContext = capacitySchedulerContext;
  }

  /**
   * 初始化所有队列，从配置解析队列层次结构。
   * @param conf 容量调度器配置
   * @throws IOException 初始化失败时抛出异常
   */
  public void initializeQueues(CapacitySchedulerConfiguration conf)
    throws IOException {
    configuredNodeLabels = new ConfiguredNodeLabels(conf);
    root = parseQueue(this.csContext.getQueueContext(), conf, null,
        CapacitySchedulerConfiguration.ROOT, queues, queues, NOOP);
    setQueueAcls(authorizer, appPriorityACLManager, queues);
    labelManager.reinitializeQueueLabels(getQueueToLabels());
    this.queueStateManager.initialize(this);
    root.updateClusterResource(csContext.getClusterResource(),
        new ResourceLimits(csContext.getClusterResource()));
    LOG.info("Initialized root queue " + root);
  }

  @Override
  /**
   * 重新初始化队列，处理配置变更更新队列结构。
   * @param newConf 新的容量调度器配置
   * @throws IOException 重新初始化失败时抛出异常
   */
  public void reinitializeQueues(CapacitySchedulerConfiguration newConf)
      throws IOException {
    // 解析新配置生成新队列结构
    CSQueueStore newQueues = new CSQueueStore();
    configuredNodeLabels = new ConfiguredNodeLabels(newConf);
    CSQueue newRoot = parseQueue(this.csContext.getQueueContext(), newConf, null,
        CapacitySchedulerConfiguration.ROOT, newQueues, queues, NOOP);

    // When failing over, if using configuration store, don't validate queue
    // hierarchy since queues can be removed without being STOPPED.
    if (!csContext.isConfigurationMutable() ||
        csContext.getRMContext().getHAServiceState()
            != HAServiceProtocol.HAServiceState.STANDBY) {
      // 验证新配置队列层次结构合法性
      CapacitySchedulerConfigValidator
              .validateQueueHierarchy(queues, newQueues, newConf);
    }

    // 验证通过后更新现有队列：添加新队列、删除已移除队列
    updateQueues(queues, newQueues);

    // 重新配置现有队列
    root.reinitialize(newRoot, this.csContext.getClusterResource());

    // 更新队列ACL权限
    setQueueAcls(authorizer, appPriorityACLManager, queues);

    // 重新计算活跃应用的可分配资源
    Resource clusterResource = this.csContext.getClusterResource();
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));

    // 重新初始化队列节点标签
    labelManager.reinitializeQueueLabels(getQueueToLabels());
    this.queueStateManager.initialize(this);
  }

  /**
   * 从配置递归解析队列层次结构。
   * @param queueContext 队列上下文
   * @param conf 容量调度器配置
   * @param parent 父队列
   * @param queueName 当前队列名称
   * @param newQueues 存储新解析的队列
   * @param oldQueues 原有队列，用于复用已有队列对象
   * @param hook 队列创建钩子
   * @return 解析完成的当前队列
   * @throws IOException 解析失败时抛出异常
   */
  static CSQueue parseQueue(
      CapacitySchedulerQueueContext queueContext, CapacitySchedulerConfiguration conf,
      CSQueue parent, String queueName, CSQueueStore newQueues, CSQueueStore oldQueues,
      QueueHook hook) throws IOException {
    CSQueue queue;
    // 构造当前队列完整路径
    QueuePath queueToParse = (parent == null) ? new QueuePath(queueName) :
        (QueuePath.createFromQueues(parent.getQueuePath(), queueName));
    // 获取当前队列的所有子队列名称
    List<String> childQueueNames = conf.getQueues(queueToParse);
    // 获取原有队列对象（如果存在）
    CSQueue oldQueue = oldQueues.get(queueToParse.getFullPath());

    // 判断队列是否可预留
    boolean isReservableQueue = conf.isReservable(queueToParse);
    // 判断是否开启子队列自动创建
    boolean isAutoCreateEnabled = conf.isAutoCreateChildQueueEnabled(queueToParse);
    // if a queue is eligible for auto queue creation v2 it must be a ParentQueue
    // (even if it is empty)
    final boolean isDynamicParent = oldQueue instanceof AbstractParentQueue &&
            oldQueue.isDynamicQueue();
    // 判断当前队列是否为支持自动创建子队列的父队列
    boolean isAutoQueueCreationEnabledParent = isDynamicParent || conf.isAutoQueueCreationV2Enabled(
        queueToParse) || isAutoCreateEnabled;

    if (childQueueNames.size() == 0 && !isAutoQueueCreationEnabledParent) {
      // 没有子队列也不允许自动创建，验证父队列合法性
      validateParent(parent, queueName);
      // Check if the queue will be dynamically managed by the Reservation system
      if (isReservableQueue) {
        // 创建预留计划队列
        queue = new PlanQueue(queueContext, queueName, parent,
            oldQueues.get(queueToParse.getFullPath()));
        ReservationQueue defaultResQueue = ((PlanQueue) queue).initializeDefaultInternalQueue();
        newQueues.add(defaultResQueue);
      } else {
        // 创建普通叶子队列
        queue = new LeafQueue(queueContext, queueName, parent,
            oldQueues.get(queueToParse.getFullPath()));
      }

      queue = hook.hook(queue);
    } else {
      if (isReservableQueue) {
        throw new IllegalStateException("Only Leaf Queues can be reservable for " +
            queueToParse.getFullPath());
      }

      AbstractParentQueue parentQueue;
      if (isAutoCreateEnabled) {
        // 创建支持自动创建子队列的托管父队列
        parentQueue = new ManagedParentQueue(queueContext, queueName, parent, oldQueues.get(
            queueToParse.getFullPath()));
      } else {
        // 创建普通静态父队列
        parentQueue = new ParentQueue(queueContext, queueName, parent, oldQueues.get(
            queueToParse.getFullPath()));
      }

      queue = hook.hook(parentQueue);
      List<CSQueue> childQueues = new ArrayList<>();
      // 递归解析所有子队列
      for (String childQueueName : childQueueNames) {
        CSQueue childQueue = parseQueue(queueContext, conf, queue, childQueueName, newQueues,
            oldQueues, hook);
        childQueues.add(childQueue);
      }

      if (!childQueues.isEmpty()) {
        // 设置子队列列表到父队列
        parentQueue.setChildQueues(childQueues);
      }

    }

    // 将当前队列添加到新队列存储
    newQueues.add(queue);

    LOG.info("Initialized queue: " + queueToParse.getFullPath());
    return queue;
  }

  /**
   * 更新现有队列集合，添加新队列、删除已移除队列，保留现有队列不变。
   * @param existingQueues 现有队列集合
   * @param newQueues 基于新配置解析的新队列集合
   */
  private void updateQueues(CSQueueStore existingQueues,
                            CSQueueStore newQueues) {
    CapacitySchedulerConfiguration conf = csContext.getConfiguration();
    // 添加所有新增队列
    for (CSQueue queue : newQueues.getQueues()) {
      if (existingQueues.get(queue.getQueuePath()) == null) {
        existingQueues.add(queue);
      }
    }

    // 检查并删除需要移除的队列
    for (CSQueue queue : existingQueues.getQueues()) {
      boolean isDanglingDynamicQueue = isDanglingDynamicQueue(
          newQueues, existingQueues, queue);
      boolean isRemovable = isDanglingDynamicQueue || !isDynamicQueue(queue)
          && newQueues.get(queue.getQueuePath()) == null
          && !(queue instanceof AutoCreatedLeafQueue &&
          conf.isAutoCreateChildQueueEnabled(queue.getParent().getQueuePathObject()));

      if (isRemovable) {
        existingQueues.remove(queue);
      }
    }

  }

  @VisibleForTesting
  /**
   * 为所有队列设置ACL权限。
   * @param authorizer YARN授权提供者
   * @param appPriorityACLManager 应用优先级ACL管理器
   * @param queues 队列存储
   * @throws IOException 设置ACL失败时抛出异常
   */
  public static void setQueueAcls(YarnAuthorizationProvider authorizer,
      AppPriorityACLsManager appPriorityACLManager, CSQueueStore queues)
      throws IOException {
    List<Permission> permissions = new ArrayList<>();
    for (CSQueue queue : queues.getQueues()) {
      AbstractCSQueue csQueue = (AbstractCSQueue) queue;
      // 添加队列本身的权限配置
      permissions.add(
          new Permission(csQueue.getPrivilegedEntity(), csQueue.getACLs()));

      if (queue instanceof AbstractLeafQueue) {
        AbstractLeafQueue lQueue = (AbstractLeafQueue) queue;

        // Clear Priority ACLs first since reinitialize also call same.
        // 清除旧的优先级ACL
        appPriorityACLManager.clearPriorityACLs(lQueue.getQueuePath());
        // 添加新的优先级ACL
        appPriorityACLManager.addPrioirityACLs(lQueue.getPriorityACLs(),
            lQueue.getQueuePath());
      }
    }
    // 批量设置权限到授权器
    authorizer.setPermission(permissions,
        UserGroupInformation.getCurrentUser());
  }

  /**
   * 获取并验证队列是叶子队列。
   * @param queue 队列名称
   * @return 验证通过返回叶子队列对象
   * @throws YarnException 队列不存在或不是叶子队列时抛出异常
   */
  public AbstractLeafQueue getAndCheckLeafQueue(String queue) throws YarnException {
    CSQueue ret = this.getQueue(queue);
    if (ret == null) {
      throw new YarnException("The specified Queue: " + queue
          + " doesn't exist");
    }
    if (!(ret instanceof AbstractLeafQueue)) {
      throw new YarnException("The specified Queue: " + queue
          + " is not a Leaf Queue.");
    }
    return (AbstractLeafQueue) ret;
  }

  /**
   * 获取队列默认应用优先级。
   * @param queueName 队列名称
   * @return 队列默认优先级，队列不存在则返回系统默认优先级
   */
  public Priority getDefaultPriorityForQueue(String queueName) {
    Queue queue = getQueue(queueName);
    if (null == queue || null == queue.getDefaultApplicationPriority()) {
      // Return with default application priority
      return Priority.newInstance(CapacitySchedulerConfiguration
          .DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);
    }
    return Priority.newInstance(queue.getDefaultApplicationPriority()
        .getPriority());
  }

  /**
   * 获取队列到可访问节点标签的映射。
   * @return 队列->标签集合映射
   */
  private Map<String, Set<String>> getQueueToLabels() {
    Map<String