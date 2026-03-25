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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 容量调度器抢占功能的可抢占队列管理类，按节点分区统计维护可被抢占杀死的容器和总资源
 */
public class PreemptableQueue {
  // 按分区统计的可杀死总资源
  private Map<String, Resource> totalKillableResources = new HashMap<>();
  // 按分区存储的可杀死容器集合，键为分区，值为容器ID到容器对象的映射
  private Map<String, Map<ContainerId, RMContainer>> killableContainers =
      new HashMap<>();
  // 父队列引用，用于层级累计统计
  private PreemptableQueue parent;

  /**
   * 构造可抢占队列，指定父队列
   * @param parent 父可抢占队列
   */
  public PreemptableQueue(PreemptableQueue parent) {
    this.parent = parent;
  }

  /**
   * 构造可抢占队列，使用已有的统计数据初始化
   * @param totalKillableResources 按分区统计的可杀死总资源
   * @param killableContainers 按分区存储的可杀死容器集合
   */
  public PreemptableQueue(Map<String, Resource> totalKillableResources,
      Map<String, Map<ContainerId, RMContainer>> killableContainers) {
    this.totalKillableResources = totalKillableResources;
    this.killableContainers = killableContainers;
  }

  /**
   * 添加一个可抢占容器到队列，并更新总资源统计
   * @param container 可杀死容器对象
   */
  void addKillableContainer(KillableContainer container) {
    String partition = container.getNodePartition();
    // 如果该分区不存在，初始化统计结构
    if (!totalKillableResources.containsKey(partition)) {
      totalKillableResources.put(partition, Resources.createResource(0));
      killableContainers.put(partition,
          new ConcurrentSkipListMap<ContainerId, RMContainer>());
    }

    RMContainer c = container.getRMContainer();
    // 将容器资源累加到分区总可杀死资源中
    Resources.addTo(totalKillableResources.get(partition),
        c.getAllocatedResource());
    // 将容器添加到可杀死容器集合
    killableContainers.get(partition).put(c.getContainerId(), c);

    // 如果存在父队列，递归添加到父队列中，实现层级统计
    if (null != parent) {
      parent.addKillableContainer(container);
    }
  }

  /**
   * 从队列中移除一个可抢占容器，并更新总资源统计
   * @param container 可杀死容器对象
   */
  void removeKillableContainer(KillableContainer container) {
    String partition = container.getNodePartition();
    Map<ContainerId, RMContainer> partitionKillableContainers =
        killableContainers.get(partition);
    if (partitionKillableContainers != null) {
      // 从集合中移除容器
      RMContainer rmContainer = partitionKillableContainers.remove(
          container.getRMContainer().getContainerId());
      // 成功移除后，从总资源中减去容器占用资源
      if (null != rmContainer) {
        Resources.subtractFrom(totalKillableResources.get(partition),
            rmContainer.getAllocatedResource());
      }
    }

    // 如果存在父队列，递归从父队列中移除容器，保持层级统计一致
    if (null != parent) {
      parent.removeKillableContainer(container);
    }
  }

  /**
   * 获取指定分区的总可杀死资源
   * @param partition 节点分区
   * @return 指定分区总可杀死资源，无数据返回空资源
   */
  public Resource getKillableResource(String partition) {
    Resource res = totalKillableResources.get(partition);
    return res == null ? Resources.none() : res;
  }

  /**
   * 获取所有分区的可杀死容器集合
   * @return 按分区组织的可杀死容器集合
   */
  public Map<String, Map<ContainerId, RMContainer>> getKillableContainers() {
    return killableContainers;
  }

  /**
   * 获取所有分区的总可杀死资源统计
   * @return 按分区组织的总可杀死资源
   */
  Map<String, Resource> getTotalKillableResources() {
    return totalKillableResources;
  }
}