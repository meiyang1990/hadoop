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

package org.apache.hadoop.yarn.server.federation.resolver;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * 子集群解析器的抽象基类，实现了SubClusterResolver接口的基础查询方法，
 * 维护节点/机架到子集群的映射关系，子类只需负责映射更新逻辑。
 */
public abstract class AbstractSubClusterResolver implements SubClusterResolver {
  // 节点名 -> 所属子集群ID的映射，线程安全实现
  private Map<String, SubClusterId> nodeToSubCluster =
      new ConcurrentHashMap<String, SubClusterId>();
  // 机架名 -> 该机架包含的所有子集群集合的映射，线程安全实现
  private Map<String, Set<SubClusterId>> rackToSubClusters =
      new ConcurrentHashMap<String, Set<SubClusterId>>();

  @Override
  public SubClusterId getSubClusterForNode(String nodename)
      throws YarnException {
    SubClusterId subClusterId = this.nodeToSubCluster.get(nodename);

    if (subClusterId == null) {
      throw new YarnException("Cannot find subClusterId for node " + nodename);
    }

    return subClusterId;
  }

  @Override
  public Set<SubClusterId> getSubClustersForRack(String rackname)
      throws YarnException {
    if (!rackToSubClusters.containsKey(rackname)) {
      throw new YarnException("Cannot resolve rack " + rackname);
    }

    return rackToSubClusters.get(rackname);
  }

  /**
   * 获取完整的节点到子集群的映射表。
   * @return 节点名->子集群ID映射表
   */
  public Map<String, SubClusterId> getNodeToSubCluster() {
    return nodeToSubCluster;
  }

  /**
   * 获取完整的机架到子集群集合的映射表。
   * @return 机架名->子集群集合映射表
   */
  public Map<String, Set<SubClusterId>> getRackToSubClusters() {
    return rackToSubClusters;
  }
}