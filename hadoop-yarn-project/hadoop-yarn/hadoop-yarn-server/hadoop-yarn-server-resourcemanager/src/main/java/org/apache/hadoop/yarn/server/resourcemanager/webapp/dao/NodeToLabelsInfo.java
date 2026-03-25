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

/**
 * 节点与标签关联信息DAO，用于ResourceManager Web UI展示节点标签分配信息
 */
@XmlRootElement(name = "nodeToLabelsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeToLabelsInfo {

  // 节点ID -> 节点标签信息映射表，键为节点id，值为该节点的标签信息
  private HashMap<String, NodeLabelsInfo> nodeToLabels =
      new HashMap<String, NodeLabelsInfo>();

  /**
   * JAXB反序列化需要的默认无参构造函数
   */
  public NodeToLabelsInfo() {
    // JAXB needs this
  }

  /**
   * 构造函数，基于已有的节点标签映射初始化
   * @param nodeToLabels 节点到标签信息的映射表
   */
  public NodeToLabelsInfo(HashMap<String, NodeLabelsInfo> nodeToLabels) {
    if (nodeToLabels != null) {
      this.nodeToLabels.putAll(nodeToLabels);
    }
  }

  /**
   * 获取所有节点与标签的映射关系
   * @return 节点到标签信息的映射表
   */
  public HashMap<String, NodeLabelsInfo> getNodeToLabels() {
    return nodeToLabels;
  }

  /**
   * 设置节点与标签的映射关系
   * @param nodeToLabels 节点到标签信息的映射表
   */
  public void setNodeToLabels(HashMap<String, NodeLabelsInfo> nodeToLabels) {
    this.nodeToLabels = nodeToLabels;
  }
}