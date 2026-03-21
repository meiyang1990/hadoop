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

import java.util.*;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.NodeLabel;

/**
 * RM Web UI节点标签信息DAO类，用于封装所有节点标签信息，支持XML/JSON序列化，
 * 供Web接口返回节点标签列表数据。
 */
@XmlRootElement(name = "nodeLabelsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeLabelsInfo {

  @XmlElement(name = "nodeLabelInfo")
  private ArrayList<NodeLabelInfo> nodeLabelsInfo = new ArrayList<>();

  /**
   * JAXB要求的无参构造函数，用于序列化/反序列化。
   */
  public NodeLabelsInfo() {
    // JAXB needs this
  }

  /**
   * 基于NodeLabelInfo列表构造NodeLabelsInfo对象。
   * @param nodeLabels 节点标签信息列表
   */
  public NodeLabelsInfo(ArrayList<NodeLabelInfo> nodeLabels) {
    this.nodeLabelsInfo = nodeLabels;
  }

  /**
   * 基于NodeLabel列表构造NodeLabelsInfo对象，转换为NodeLabelInfo格式存储。
   * @param nodeLabels API层NodeLabel列表
   */
  public NodeLabelsInfo(List<NodeLabel> nodeLabels) {
    this.nodeLabelsInfo = new ArrayList<>();
    for (NodeLabel label : nodeLabels) {
      this.nodeLabelsInfo.add(new NodeLabelInfo(label));
    }
  }

  /**
   * 基于标签名称集合构造NodeLabelsInfo对象。
   * @param nodeLabelsName 节点标签名称集合
   */
  public NodeLabelsInfo(Set<String> nodeLabelsName) {
    this.nodeLabelsInfo = new ArrayList<>();
    for (String labelName : nodeLabelsName) {
      this.nodeLabelsInfo.add(new NodeLabelInfo(labelName));
    }
  }

  /**
   * 基于NodeLabel集合构造NodeLabelsInfo对象，转换为NodeLabelInfo格式存储。
   * @param nodeLabels API层NodeLabel集合
   */
  public NodeLabelsInfo(Collection<NodeLabel> nodeLabels) {
    this.nodeLabelsInfo = new ArrayList<>();
    nodeLabels.stream().forEach(nodeLabel -> {
      this.nodeLabelsInfo.add(new NodeLabelInfo(nodeLabel));
    });
  }

  /**
   * 获取所有节点标签信息列表。
   * @return 节点标签信息列表
   */
  public ArrayList<NodeLabelInfo> getNodeLabelsInfo() {
    return nodeLabelsInfo;
  }

  /**
   * 将当前存储的NodeLabelInfo转换回API层NodeLabel集合返回。
   * @return API层NodeLabel集合
   */
  public Set<NodeLabel> getNodeLabels() {
    Set<NodeLabel> nodeLabels = new HashSet<>();
    for (NodeLabelInfo label : nodeLabelsInfo) {
      nodeLabels.add(NodeLabel.newInstance(label.getName(),
          label.getExclusivity()));
    }
    return nodeLabels;
  }

  /**
   * 获取所有节点标签名称列表。
   * @return 节点标签名称列表
   */
  public List<String> getNodeLabelsName() {
    ArrayList<String> nodeLabelsName = new ArrayList<>();
    for (NodeLabelInfo label : nodeLabelsInfo) {
      nodeLabelsName.add(label.getName());
    }
    return nodeLabelsName;
  }

  /**
   * 设置节点标签信息列表。
   * @param nodeLabelInfo 节点标签信息列表
   */
  public void setNodeLabelsInfo(ArrayList<NodeLabelInfo> nodeLabelInfo) {
    this.nodeLabelsInfo = nodeLabelInfo;
  }
}