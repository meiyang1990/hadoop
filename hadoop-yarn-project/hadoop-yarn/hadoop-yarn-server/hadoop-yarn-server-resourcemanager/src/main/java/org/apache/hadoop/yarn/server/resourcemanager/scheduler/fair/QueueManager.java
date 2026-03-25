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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 公平调度器队列管理器，维护所有调度队列的层级结构、配置参数和动态生命周期管理
 */
@Private
@Unstable
public class QueueManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueueManager.class.getName());

  /**
   * 不兼容队列移除任务，用于等待队列变为空闲后再执行删除
   */
  private final class IncompatibleQueueRemovalTask {

    /** 待创建队列名称 */
    private final String queueToCreate;
    /** 待创建队列类型 */
    private final FSQueueType queueType;

    private IncompatibleQueueRemovalTask(String queueToCreate,
        FSQueueType queueType) {
      this.queueToCreate = queueToCreate;
      this.queueType = queueType;
    }

    /**
     * 尝试移除不兼容的空队列，创建目标队列
     */
    private void execute() {
      Boolean removed =
          removeEmptyIncompatibleQueues(queueToCreate, queueType).orElse(null);
      if (Boolean.TRUE.equals(removed)) {
        FSQueue queue = getQueue(queueToCreate, true, queueType, false, null);
        if (queue != null &&
            // 如果目标队列在分配配置中存在，则标记为静态队列
            scheduler.allocConf.configuredQueues.values().stream()
            .anyMatch(s -> s.contains(queueToCreate))) {
          queue.setDynamic(false);
        }
      }
      if (!Boolean.FALSE.equals(removed)) {
        incompatibleQueuesPendingRemoval.remove(this);
      }
    }
  }

  public static final String ROOT_QUEUE = "root";
  
  private final FairScheduler scheduler;

  // 所有叶子队列列表，支持并发读取
  private final Collection<FSLeafQueue> leafQueues = 
      new CopyOnWriteArrayList<>();
  // 全量队列映射，队列全名 -> 队列对象
  private final Map<String, FSQueue> queues = new HashMap<>();
  // 待移除的不兼容队列任务集合，当前非空无法删除，等待后续处理
  private Set<IncompatibleQueueRemovalTask> incompatibleQueuesPendingRemoval =
      new HashSet<>();
  // 根队列对象
  private FSParentQueue rootQueue;

  /**
   * 构造队列管理器，绑定所属公平调度器实例
   * @param scheduler 公平调度器实例
   */
  public QueueManager(FairScheduler scheduler) {
    this.scheduler = scheduler;
  }
  
  public FSParentQueue getRootQueue() {
    return rootQueue;
  }

  /**
   * 初始化队列管理器，创建根队列并完成初始化
   */
  public void initialize() {
    // 根队列和默认队列的策略先设置为默认策略，等待分配配置文件加载后再更新
    rootQueue = new FSParentQueue("root", scheduler, null);
    rootQueue.setDynamic(false);
    queues.put(rootQueue.getName(), rootQueue);

    // 递归重新初始化，传播队列属性
    rootQueue.reinit(true);
  }

  /**
   * Get a leaf queue by name, creating it if the create param is
   * <code>true</code> and the queue does not exist.
   * If the queue is not or can not be a leaf queue, i.e. it already exists as
   * a parent queue, or one of the parents in its name is already a leaf queue,
   * <code>null</code> is returned.
   * 
   * The root part of the name is optional, so a queue underneath the root 
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root 
   * could be referred to as just "parent1.queue2".
   * @param name name of the queue
   * @param create <code>true</code> if the queue must be created if it does
   *               not exist, <code>false</code> otherwise
   * @return the leaf queue or <code>null</code> if the queue cannot be found
   */
  public FSLeafQueue getLeafQueue(String name, boolean create) {
    return getLeafQueue(name, create, null, true);
  }

  /**
   * Get a leaf queue by name, creating it if the create param is
   * <code>true</code> and the queue does not exist.
   * If the queue is not or can not be a leaf queue, i.e. it already exists as
   * a parent queue, or one of the parents in its name is already a leaf queue,
   * <code>null</code> is returned.
   *
   * If the application will be assigned to the queue if the applicationId is
   * not <code>null</code>
   * @param name name of the queue
   * @param create <code>true</code> if the queue must be created if it does
   *               not exist, <code>false</code> otherwise
   * @param applicationId the application ID to assign to the queue
   * @return the leaf queue or <code>null</code> if teh queue cannot be found
   */
  public FSLeafQueue getLeafQueue(String name, boolean create,
                                  ApplicationId applicationId) {
    return getLeafQueue(name, create, applicationId, true);
  }

  private FSLeafQueue getLeafQueue(String name, boolean create,
                                   ApplicationId applicationId,
                                   boolean recomputeSteadyShares) {
    FSQueue queue = getQueue(name, create, FSQueueType.LEAF,
        recomputeSteadyShares, applicationId);
    if (queue instanceof FSParentQueue) {
      return null;
    }
    return (FSLeafQueue) queue;
  }

  /**
   * Remove a leaf queue if empty.
   * @param name name of the queue
   * @return true if queue was removed or false otherwise
   */
  public boolean removeLeafQueue(String name) {
    name = ensureRootPrefix(name);
    return !Boolean.FALSE.equals(
        removeEmptyIncompatibleQueues(name, FSQueueType.PARENT).orElse(null));
  }


  /**
   * Get a parent queue by name, creating it if the create param is
   * <code>true</code> and the queue does not exist.
   * If the queue is not or can not be a parent queue, i.e. it already exists
   * as a leaf queue, or one of the parents in its name is already a leaf
   * queue, <code>null</code> is returned.
   * 
   * The root part of the name is optional, so a queue underneath the root 
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root 
   * could be referred to as just "parent1.queue2".
   * @param name name of the queue
   * @param create <code>true</code> if the queue must be created if it does
   *               not exist, <code>false</code> otherwise
   * @return the parent queue or <code>null</code> if the queue cannot be found
   */
  public FSParentQueue getParentQueue(String name, boolean create) {
    return getParentQueue(name, create, true);
  }

  /**
   * Get a parent queue by name, creating it if the create param is
   * <code>true</code> and the queue does not exist.
   * If the queue is not or can not be a parent queue, i.e. it already exists
   * as a leaf queue, or one of the parents in its name is already a leaf
   * queue, <code>null</code> is returned.
   *
   * The root part of the name is optional, so a queue underneath the root
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root
   * could be referred to as just "parent1.queue2".
   * @param name name of the queue
   * @param create <code>true</code> if the queue must be created if it does
   *               not exist, <code>false</code> otherwise
   * @param recomputeSteadyShares <code>true</code> if the steady fair share
   *                              should be recalculated when a queue is added,
   *                              <code>false</code> otherwise
   * @return the parent queue or <code>null</code> if the queue cannot be found
   */
  public FSParentQueue getParentQueue(String name, boolean create,
      boolean recomputeSteadyShares) {
    FSQueue queue = getQueue(name, create, FSQueueType.PARENT,
        recomputeSteadyShares, null);
    if (queue instanceof FSLeafQueue) {
      return null;
    }
    return (FSParentQueue) queue;
  }

  /**
   * 根据队列名获取或创建队列，处理不存在队列的创建逻辑
   * @param name 队列名
   * @param create 是否创建不存在的队列
   * @param queueType 队列类型（叶子/父队列）
   * @param recomputeSteadyShares 是否需要重新计算稳定公平份额
   * @param applicationId 要分配到该队列的应用ID
   * @return 队列对象，创建失败返回null
   */
  private FSQueue getQueue(String name, boolean create, FSQueueType queueType,
      boolean recomputeSteadyShares, ApplicationId applicationId) {
    boolean recompute = recomputeSteadyShares;
    // 统一补全root前缀，支持用户省略root写法
    name = ensureRootPrefix(name);
    FSQueue queue;
    synchronized (queues) {
      queue = queues.get(name);
      if (queue == null && create) {
        // 队列不存在，创建新队列并返回
        queue = createQueue(name, queueType);
      } else {
        // 队列已存在，不需要重新计算份额
        recompute = false;
      }
      // 如果提供了应用ID且队列是叶子队列，将应用分配到该队列
      if (applicationId != null && queue instanceof FSLeafQueue) {
        ((FSLeafQueue)queue).addAssignedApp(applicationId);
      }
    }
    // 如果是新创建队列，重新计算全量稳定公平份额
    if (recompute && queue != null) {
      rootQueue.recomputeSteadyShares();
    }
    return queue;
  }

  /**
   * Create a leaf or parent queue based on what is specified in
   * {@code queueType} and place it in the tree. Create any parents that don't
   * already exist.
   * 
   * @return the created queue, if successful or null if not allowed (one of the
   * parent queues in the queue name is already a leaf queue)
   */
  @VisibleForTesting
  FSQueue createQueue(String name, FSQueueType queueType) {
    List<String> newQueueNames = new ArrayList<>();
    FSParentQueue parent = buildNewQueueList(name, newQueueNames);
    FSQueue queue = null;

    if (parent != null) {
      // 路径校验通过，创建所有缺失的父队列和目标队列
      queue = createNewQueues(queueType, parent, newQueueNames);
    }

    return queue;
  }

  /**
   * Compile a list of all parent queues of the given queue name that do not
   * already exist. The queue names will be added to the {@code newQueueNames}
   * list. The list will be in order of increasing queue depth. The first
   * element of the list will be the parent closest to the root. The last
   * element added will be the queue to be created. This method returns the
   * deepest parent that does exist.
   *
   * @param name the fully qualified name of the queue to create
   * @param newQueueNames the list to which to add non-existent queues
   * @return the deepest existing parent queue
   */
  private FSParentQueue buildNewQueueList(String name,
      List<String> newQueueNames) {
    newQueueNames.add(name);
    int sepIndex = name.length();
    FSParentQueue parent = null;

    // 向上遍历队列路径，直到找到已存在的父队列
    while (sepIndex != -1) {
      int prevSepIndex = sepIndex;
      sepIndex = name.lastIndexOf('.', sepIndex-1);
      String node = name.substring(sepIndex+1, prevSepIndex);
      // 校验队列节点名称合法性
      if (!isQueueNameValid(node)) {
        throw new InvalidQueueNameException("Illegal node name at offset " +
            (sepIndex+1) + " for queue name " + name);
      }

      String curName = name.substring(0, sepIndex);
      FSQueue queue = queues.get(curName);

      if (queue == null) {
        // 当前层级队列不存在，添加到待创建列表
        newQueueNames.add(0, curName);
      } else {
        // 找到已存在的父队列，检查是否为父队列类型
        if (queue instanceof FSParentQueue) {
          parent = (FSParentQueue)queue;
        }

        // 如果找到的已存在队列不是父队列，parent保持null

        break;
      }
    }

    return parent;
  }

  /**
   * Create all queues in the {@code newQueueNames} list. The list must be in
   * order of increasing depth. All but the last element in the list will be
   * created as parent queues. The last element will be created as the type
   * specified by the {@code queueType} parameter. The first queue will be
   * created as a child of the {@code topParent} queue. All subsequent queues
   * will be created as a child of the previously created queue.
   *
   * @param queueType the type of the last queue to create
   * @param topParent the parent of the first queue to create
   * @param newQueueNames the list of queues to create
   * @return the last queue created
   */
  private FSQueue createNewQueues(FSQueueType queueType,
      FSParentQueue topParent, List<String> newQueueNames) {
    AllocationConfiguration queueConf = scheduler.getAllocationConfiguration();
    Iterator<String> i = newQueueNames.iterator();
    FSParentQueue parent = topParent;
    FSQueue queue = null;

    while (i.hasNext()) {
      FSParentQueue newParent = null;
      String queueName = i.next();

      // 检查子队列调度策略是否被父队列允许
      SchedulingPolicy childPolicy = scheduler.getAllocationConfiguration().
          getSchedulingPolicy(queueName);
      if (!parent.getPolicy().isChildPolicyAllowed(childPolicy)) {
        LOG.error("Can't create queue '" + queueName + "'," +
                "the child scheduling policy is not allowed by parent queue!");
        return null;
      }

      // 只有最后一个节点可以是叶子队列
      if (!i.hasNext() && (queueType != FSQueueType.PARENT)) {
        // 创建叶子队列
        FSLeafQueue leafQueue = new FSLeafQueue(queueName, scheduler, parent);
        leafQueues.add(leafQueue);
        queue = leafQueue;
      } else {
        // 中间节点必须是父队列，检查FIFO策略只能用于叶子队列
        if (childPolicy instanceof FifoPolicy) {
          LOG.error("Can't create queue '" + queueName + "', since "
              + FifoPolicy.NAME + " is only for leaf queues.");
          return null;
        }
        // 创建父队列
        newParent = new FSParentQueue(queueName, scheduler, parent);
        queue = newParent;
      }

      // 将新队列添加到父队列的子队列列表
      parent.addChildQueue(queue);
      // 根据父队列默认配置