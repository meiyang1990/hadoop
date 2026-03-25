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

import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * YARN联邦环境下的子集群解析器接口，用于查询指定节点或机架所属的子集群。
 * 所有实现类必须保证线程安全。
 */
public interface SubClusterResolver extends Configurable {

  /**
   * 获取指定节点所属的子集群ID。
   *
   * @param nodename 待查询的节点名称
   * @return 节点所属子集群的ID
   * @throws YarnException 当无法解析节点所属子集群时抛出异常
   */
  SubClusterId getSubClusterForNode(String nodename) throws YarnException;

  /**
   * 获取指定机架上所有节点所属的子集群集合。
   *
   * @param rackname 待查询的机架名称
   * @return 该机架上存在节点的所有子集群ID集合
   * @throws YarnException 当机架名称不合法或无法解析机架上任意节点的子集群信息时抛出异常
   */
  Set<SubClusterId> getSubClustersForRack(String rackname) throws YarnException;

  /**
   * 从配置文件加载节点与子集群的映射关系。
   */
  void load();
}