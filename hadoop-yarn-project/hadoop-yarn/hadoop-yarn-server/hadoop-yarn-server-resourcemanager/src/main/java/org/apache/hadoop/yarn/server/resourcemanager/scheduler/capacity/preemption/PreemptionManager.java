// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 容量调度器抢占管理器，管理所有可被抢占杀死的容器，按队列组织可抢占资源，支持线程安全的并发访问
 */
public class PreemptionManager {
  private ReentrantReadWriteLock.ReadLock readLock;
  private ReentrantReadWriteLock.WriteLock writeLock;
  // 按队列路径存储可抢占队列信息
  private Map<String, PreemptableQueue> entities = new HashMap<>();

  /**
   * 构造函数，初始化读写锁用于线程安全访问
   */
  public PreemptionManager() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  /**
   * 刷新队列结构，递归遍历调度队列树，注册所有可被抢占的队列
   * @param parent 父队列
   * @param current 当前处理队列
   */
  public void refreshQueues(CSQueue parent, CSQueue current) {
    // 获取写锁，保证队列结构修改线程安全
    writeLock.lock();
    try {
      PreemptableQueue parentEntity = null;
      if (parent != null) {
        // 从已注册队列中获取父队列实体
        parentEntity = entities.get(parent.getQueuePath());
      }

      // 如果当前队列未注册，则添加到可抢占队列映射
      if (!entities.containsKey(current.getQueuePath())) {
        entities.put(current.getQueuePath(),
            new PreemptableQueue(parentEntity));
      }

      // 获取当前队列的子队列列表
      List<CSQueue> childQueues = current.getChildQueuesByTryLock();
      if (childQueues != null) {
        // 递归处理所有子队列
        for (CSQueue child : childQueues) {
          refreshQueues(current, child);
        }
      }
    }
    finally {
      // 释放写锁
      writeLock.unlock();
    }
  }

  /**
   * 添加一个可被杀死的容器到对应队列
   * @param container 可杀死容器对象
   */
  public void addKillableContainer(KillableContainer container) {
    writeLock.lock();
    try {
      PreemptableQueue entity = entities.get(container.getLeafQueueName());
      if (null != entity) {
        entity.addKillableContainer(container);
      }
    }
    finally {
      writeLock.unlock();
    }
  }

  /**
   * 从对应队列移除一个可被杀死的容器
   * @param container 可杀死容器对象
   */
  public void removeKillableContainer(KillableContainer container) {
    writeLock.lock();
    try {
      PreemptableQueue entity = entities.get(container.getLeafQueueName());
      if (null != entity) {
        entity.removeKillableContainer(container);
      }
    }
    finally {
      writeLock.unlock();
    }
  }

  /**
   * 移动可杀死容器（节点分区变更或容器换队列时调用，待实现）
   * @param oldContainer 原容器对象
   * @param newContainer 新容器对象
   */
  public void moveKillableContainer(KillableContainer oldContainer,
      KillableContainer newContainer) {
    // TODO, will be called when partition of the node changed OR
    // container moved to different queue
  }

  /**
   * 更新可杀死容器资源（容器资源变更时调用，待实现）
   * @param container 可杀死容器对象
   * @param oldResource 旧资源
   * @param newResource 新资源
   */
  public void updateKillableContainerResource(KillableContainer container,
      Resource oldResource, Resource newResource) {
    // TODO, will be called when container's resource changed
  }

  @VisibleForTesting
  /**
   * 获取指定队列指定分区的可杀死容器映射（仅测试用）
   * @param queueName 队列名称
   * @param partition 节点分区
   * @return 可杀死容器ID到RM容器的映射
   */
  public Map<ContainerId, RMContainer> getKillableContainersMap(
      String queueName, String partition) {
    readLock.lock();
    try {
      PreemptableQueue entity = entities.get(queueName);
      if (entity != null) {
        Map<ContainerId, RMContainer> containers =
            entity.getKillableContainers().get(partition);
        if (containers != null) {
          return containers;
        }
      }
      return Collections.emptyMap();
    }
    finally {
      readLock.unlock();
    }
  }

  /**
   * 获取指定队列指定分区的可杀死容器迭代器
   * @param queueName 队列名称
   * @param partition 节点分区
   * @return 可杀死RM容器迭代器
   */
  public Iterator<RMContainer> getKillableContainers(String queueName,
      String partition) {
    return getKillableContainersMap(queueName, partition).values().iterator();
  }

  /**
   * 获取指定队列指定分区的总可抢占资源量
   * @param queueName 队列名称
   * @param partition 节点分区
   * @return 可抢占资源总量
   */
  public Resource getKillableResource(String queueName, String partition) {
    readLock.lock();
    try {
      PreemptableQueue entity = entities.get(queueName);
      if (entity != null) {
        Resource res = entity.getTotalKillableResources().get(partition);
        if (res == null || res.equals(Resources.none())) {
          return Resources.none();
        }
        return Resources.clone(res);
      }
      return Resources.none();
    }
    finally {
      readLock.unlock();
    }
  }

  /**
   * 获取所有可抢占队列的浅拷贝，用于抢占计算
   * @return 可抢占队列映射的浅拷贝（内部数据做深度拷贝）
   */
  public Map<String, PreemptableQueue> getShallowCopyOfPreemptableQueues() {
    readLock.lock();
    try {
      Map<String, PreemptableQueue> map = new HashMap<>();
      // 遍历所有已注册队列，复制可抢占资源和容器信息
      for (Map.Entry<String, PreemptableQueue> entry : entities.entrySet()) {
        String key = entry.getKey();
        PreemptableQueue entity = entry.getValue();
        map.put(key, new PreemptableQueue(
            new HashMap<>(entity.getTotalKillableResources()),
            new HashMap<>(entity.getKillableContainers())));
      }
      return map;
    } finally {
      readLock.unlock();
    }
  }
}