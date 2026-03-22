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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

/**
 * HDFS数据平衡器节点匹配接口，定义判断两个数据节点是否符合匹配条件的契约。
 * 用于在平衡过程中筛选符合拓扑位置要求的源/目标数据节点对。
 */
public interface Matcher {
  /**
   * 判断给定集群拓扑中两个节点是否符合匹配条件。
   * @param cluster 集群网络拓扑信息
   * @param left 待匹配的第一个节点
   * @param right 待匹配的第二个节点
   * @return 两个节点符合匹配条件返回true，否则返回false
   */
  public boolean match(NetworkTopology cluster, Node left,  Node right);

  /**
   * 匹配同一节点组内的两个数据节点。
   * 用于优先在同一节点组内进行数据块移动，减少跨节点组流量开销。
   */
  public static final Matcher SAME_NODE_GROUP = new Matcher() {
    @Override
    public boolean match(NetworkTopology cluster, Node left, Node right) {
      return cluster.isOnSameNodeGroup(left, right);
    }

    @Override
    public String toString() {
      return "SAME_NODE_GROUP";
    }
  };

  /**
   * 匹配同一机架内的两个数据节点。
   * 用于优先在同一机架内进行数据块移动，减少跨机架流量开销。
   */
  public static final Matcher SAME_RACK = new Matcher() {
    @Override
    public boolean match(NetworkTopology cluster, Node left, Node right) {
      return cluster.isOnSameRack(left, right);
    }

    @Override
    public String toString() {
      return "SAME_RACK";
    }
  };

  /**
   * 匹配任意两个不同的数据节点。
   * 允许跨节点组、跨机架进行数据块移动，用于完成全局负载均衡。
   */
  public static final Matcher ANY_OTHER = new Matcher() {
    @Override
    public boolean match(NetworkTopology cluster, Node left, Node right) {
      return left != right;
    }

    @Override
    public String toString() {
      return "ANY_OTHER";
    }
  };
}