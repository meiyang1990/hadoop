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

import java.util.ArrayList;
import java.util.Collection;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN RM Web UI 节点信息列表数据访问对象，封装所有节点信息用于REST接口返回
 */
@XmlRootElement(name = "nodes")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodesInfo {

  protected ArrayList<NodeInfo> node = new ArrayList<NodeInfo>();

  /**
   * 无参构造函数，供JAXB序列化/反序列化使用
   */
  public NodesInfo() {
  } // JAXB needs this

  /**
   * 添加单个节点信息到列表
   * @param nodeinfo 单个节点信息对象
   */
  public void add(NodeInfo nodeinfo) {
    node.add(nodeinfo);
  }

  /**
   * 获取所有节点信息列表
   * @return 所有节点信息的ArrayList
   */
  public ArrayList<NodeInfo> getNodes() {
    return node;
  }

  /**
   * 批量添加多个节点信息（ArrayList版本）
   * @param nodesInfo 待添加的节点信息列表
   */
  public void addAll(ArrayList<NodeInfo> nodesInfo) {
    node.addAll(nodesInfo);
  }

  /**
   * 批量添加多个节点信息（Collection通用版本）
   * @param nodesInfo 待添加的节点信息集合
   */
  public void addAll(Collection<NodeInfo> nodesInfo) {
    node.addAll(nodesInfo);
  }
}