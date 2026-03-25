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
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlElement;

/**
 * 节点标签关联关系数据访问对象，用于RM WebUI REST API返回节点及其标签映射信息
 */
@XmlRootElement(name = "nodeToLabelsEntry")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeToLabelsEntry {

  @XmlElement(name = "nodeId")
  private String nodeId;

  @XmlElement(name = "labels")
  private ArrayList<String> labels = new ArrayList<String>();

  /**
   * JAXB反序列化所需的无参构造函数
   */
  public NodeToLabelsEntry() {
    // JAXB needs this
  }

  /**
   * 构造节点标签映射条目
   * @param nodeId 节点ID
   * @param labels 节点关联的标签列表
   */
  public NodeToLabelsEntry(String nodeId, ArrayList<String> labels) {
    this.nodeId = nodeId;
    this.labels = labels;
  }

  /**
   * 构造节点标签映射条目，从集合拷贝标签
   * @param nodeId 节点ID
   * @param pLabels 节点关联的标签集合
   */
  public NodeToLabelsEntry(String nodeId, Collection<String> pLabels) {
    this.nodeId = nodeId;
    this.labels.addAll(pLabels);
  }

  /**
   * 获取节点ID
   * @return 节点ID
   */
  public String getNodeId() {
    return nodeId;
  }

  /**
   * 获取节点关联的所有标签
   * @return 节点标签列表
   */
  public ArrayList<String> getNodeLabels() {
    return labels;
  }
}