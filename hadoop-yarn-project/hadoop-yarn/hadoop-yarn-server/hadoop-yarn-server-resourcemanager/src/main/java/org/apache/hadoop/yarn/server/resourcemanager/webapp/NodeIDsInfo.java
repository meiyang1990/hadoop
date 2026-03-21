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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Collection;
import java.util.HashSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * JAXB 数据绑定类，用于在RM Web API中返回节点ID列表及关联分区信息
 */
@XmlRootElement(name = "nodeIDsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeIDsInfo {

  /**
   * 存储节点ID列表，使用ArrayList兼容JAXB无参构造要求
   */
  @XmlElement(name="nodes")
  protected ArrayList<String> nodeIDsList = new ArrayList<String>();

  /**
   * 关联的资源分区信息
   */
  @XmlElement(name = "partitionInfo")
  private PartitionInfo partitionInfo;

  /** JAXB要求的无参构造函数 */
  public NodeIDsInfo() {
  } // JAXB needs this

  /**
   * 构造仅包含节点ID列表的NodeIDsInfo对象
   * @param nodeIdsList 节点ID列表
   */
  public NodeIDsInfo(List<String> nodeIdsList) {
    this.nodeIDsList.addAll(nodeIdsList);
  }

  /**
   * 构造包含节点ID列表和资源信息的NodeIDsInfo对象
   * @param nodeIdsList 节点ID列表
   * @param resource 资源信息
   */
  public NodeIDsInfo(List<String> nodeIdsList, Resource resource) {
    this(nodeIdsList);
    this.partitionInfo = new PartitionInfo(new ResourceInfo(resource));
  }

  /**
   * 构造包含节点ID列表和分区信息的NodeIDsInfo对象
   * @param nodeIdsList 节点ID集合
   * @param partitionInfo 资源分区信息
   */
  public NodeIDsInfo(Collection<String> nodeIdsList, PartitionInfo partitionInfo) {
    this.nodeIDsList.addAll(nodeIdsList);
    this.partitionInfo = partitionInfo;
  }

  /**
   * 获取节点ID列表
   * @return 节点ID列表
   */
  public ArrayList<String> getNodeIDs() {
    return nodeIDsList;
  }

  /**
   * 获取资源分区信息
   * @return 资源分区信息
   */
  public PartitionInfo getPartitionInfo() {
    return partitionInfo;
  }

  /**
   * 合并两个NodeIDsInfo对象，合并时自动去重节点ID并累加分区资源信息
   *
   * @param left 待合并的第一个NodeIDsInfo对象
   * @param right 待合并的第二个NodeIDsInfo对象
   * @return 合并后的新NodeIDsInfo对象
   */
  public static NodeIDsInfo add(NodeIDsInfo left, NodeIDsInfo right) {
    // 使用Set去重合并两个对象的节点ID
    Set<String> nodes = new HashSet<>();
    if (left != null && left.nodeIDsList != null) {
      nodes.addAll(left.nodeIDsList);
    }
    if (right != null && right.nodeIDsList != null) {
      nodes.addAll(right.nodeIDsList);
    }

    // 分别获取两个对象的分区信息
    PartitionInfo leftPartitionInfo = null;
    if (left != null) {
      leftPartitionInfo = left.getPartitionInfo();
    }

    PartitionInfo rightPartitionInfo = null;
    if (right != null) {
      rightPartitionInfo = right.getPartitionInfo();
    }

    // 合并分区资源信息
    PartitionInfo info = PartitionInfo.addTo(leftPartitionInfo, rightPartitionInfo);
    return new NodeIDsInfo(nodes, info);
  }
}