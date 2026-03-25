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

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodeIDsInfo;

/**
 * YARN ResourceManager Web UI 数据访问对象，存储节点标签到对应节点ID列表的映射信息
 * 用于REST API返回标签与节点关联关系数据
 */
@XmlRootElement(name = "labelsToNodesInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class LabelsToNodesInfo {

  // 存储节点标签信息到对应节点ID列表的映射
  private Map<NodeLabelInfo, NodeIDsInfo> labelsToNodes = new HashMap<>();

  /**
   * JAXB反序列化所需的无参构造函数
   */
  public LabelsToNodesInfo() {
  } // JAXB needs this

  /**
   * 带标签节点映射的构造函数
   * @param labelsToNodes 节点标签到对应节点ID列表的映射
   */
  public LabelsToNodesInfo(Map<NodeLabelInfo, NodeIDsInfo> labelsToNodes) {
    this.labelsToNodes = labelsToNodes;
  }

  /**
   * 获取标签到节点的映射关系
   * @return 标签与对应节点ID列表的映射
   */
  public Map<NodeLabelInfo, NodeIDsInfo> getLabelsToNodes() {
    return labelsToNodes;
  }

  /**
   * 设置标签到节点的映射关系
   * @param labelsToNodes 标签与对应节点ID列表的映射
   */
  public void setLabelsToNodes(Map<NodeLabelInfo, NodeIDsInfo> labelsToNodes) {
    this.labelsToNodes = labelsToNodes;
  }
}