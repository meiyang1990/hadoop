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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterNodeTracker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 公平调度器中已访问资源请求追踪器，用于追踪同一优先级资源下不同位置层级的资源请求是否已被访问
 * 
 * 专为{@link FSAppAttempt#getStarvedResourceRequests()}实现，用于识别饥饿资源请求
 * 该实现非线程安全
 */
class VisitedResourceRequestTracker {
  private static final Logger LOG =
      LoggerFactory.getLogger(VisitedResourceRequestTracker.class);
  // 按优先级分组，每个优先级下按资源容量分组存储追踪器
  private final Map<Priority, Map<Resource, TrackerPerPriorityResource>> map =
      new HashMap<>();
  // 集群节点信息追踪器，用于查询节点所属机架信息
  private final ClusterNodeTracker<FSSchedulerNode> nodeTracker;

  /**
   * 构造函数，初始化追踪器
   * @param nodeTracker 集群节点追踪器
   */
  VisitedResourceRequestTracker(
      ClusterNodeTracker<FSSchedulerNode> nodeTracker) {
    this.nodeTracker = nodeTracker;
  }

  /**
   * 标记当前资源请求为已访问，并判断是否为所有位置层级中首次访问
   * @param rr 待访问的资源请求
   * @return true表示为首次访问，false表示已访问过
   */
  boolean visit(ResourceRequest rr) {
    Priority priority = rr.getPriority();
    Resource capability = rr.getCapability();

    Map<Resource, TrackerPerPriorityResource> subMap = map.get(priority);
    // 当前优先级无记录，创建新分组
    if (subMap == null) {
      subMap = new HashMap<>();
      map.put(priority, subMap);
    }

    TrackerPerPriorityResource tracker = subMap.get(capability);
    // 当前优先级和资源容量无记录，创建新追踪器
    if (tracker == null) {
      tracker = new TrackerPerPriorityResource();
      subMap.put(capability, tracker);
    }

    return tracker.visit(rr.getResourceName());
  }

  /**
   * 同一优先级同一资源容量下的访问追踪器，按位置层级（节点/机架/ANY）追踪访问状态
   */
  private class TrackerPerPriorityResource {
    // 已访问过节点的机架集合
    private Set<String> racksWithNodesVisited = new HashSet<>();
    // 已访问过机架的集合
    private Set<String> racksVisited = new HashSet<>();
    // ANY位置是否已访问
    private boolean anyVisited;

    /**
     * 标记ANY位置为已访问，判断是否首次访问
     * @return 首次访问返回true，否则false
     */
    private boolean visitAny() {
      // ANY只有在所有节点/机架都未访问时才视为首次访问
      if (racksVisited.isEmpty() && racksWithNodesVisited.isEmpty()) {
        anyVisited = true;
      }
      return anyVisited;
    }

    /**
     * 标记机架为已访问，判断是否首次访问
     * @param rackName 机架名称
     * @return 首次访问返回true，否则false
     */
    private boolean visitRack(String rackName) {
      // ANY已访问或该机架已有节点被访问，视为已访问
      if (anyVisited || racksWithNodesVisited.contains(rackName)) {
        return false;
      } else {
        racksVisited.add(rackName);
        return true;
      }
    }

    /**
     * 标记节点所在机架为已访问节点，判断是否首次访问
     * @param rackName 节点所属机架名称
     * @return 首次访问返回true，否则false
     */
    private boolean visitNode(String rackName) {
      // ANY已访问或该机架已被访问，视为已访问
      if (anyVisited || racksVisited.contains(rackName)) {
        return false;
      } else {
        racksWithNodesVisited.add(rackName);
        return true;
      }
    }

    /**
     * 根据资源名称（节点/机架/ANY）判断并标记访问状态
     * 
     * 访问规则：
     * 节点：其机架或ANY已访问则视为已访问
     * 机架：该机架任意节点或ANY已访问则视为已访问
     * ANY：任意节点/机架已访问则视为已访问
     *
     * @param resourceName 资源名称，可为节点名、机架名或ANY
     * @return true为首次访问，false已访问过
     */
    private boolean visit(String resourceName) {
      if (resourceName.equals(ResourceRequest.ANY)) {
        return visitAny();
      }

      // 根据资源名称查询匹配的节点列表
      List<FSSchedulerNode> nodes =
          nodeTracker.getNodesByResourceName(resourceName);
      int numNodes = nodes.size();
      // 未找到匹配节点，记录错误日志返回已访问
      if (numNodes == 0) {
        LOG.error("Found ResourceRequest for a non-existent node/rack named " +
            resourceName);
        return false;
      }

      // 仅匹配到一个节点，需确认是节点还是单节点机架
      if (numNodes == 1) {
        FSSchedulerNode node = nodes.get(0);
        // 资源名称等于节点名，说明请求是针对节点的
        if (node.getNodeName().equals(resourceName)) {
          return visitNode(node.getRackName());
        }
      }

      // 既不是ANY也不是节点，认定为机架请求
      return visitRack(resourceName);
    }
  }
}