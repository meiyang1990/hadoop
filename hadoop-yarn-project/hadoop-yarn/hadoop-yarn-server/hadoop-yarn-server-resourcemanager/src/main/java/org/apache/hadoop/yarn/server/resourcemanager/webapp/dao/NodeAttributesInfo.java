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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

/**
 * YARN RM WebAPI 节点属性信息列表的数据访问对象（DAO）
 * 用于封装所有节点属性信息，供Web服务序列化为XML/JSON返回给前端
 */
@XmlRootElement(name = "nodeAttributesInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeAttributesInfo {

  @XmlElement(name = "nodeAttributeInfo")
  private ArrayList<NodeAttributeInfo> nodeAttributesInfo =
      new ArrayList<>();

  /**
   * JAXB序列化需要的无参构造函数
   */
  public NodeAttributesInfo() {
    // JAXB needs this
  }

  /**
   * 添加单个节点属性信息到列表
   * @param attributeInfo 单个节点属性信息对象
   */
  public void addNodeAttributeInfo(NodeAttributeInfo attributeInfo) {
    this.nodeAttributesInfo.add(attributeInfo);
  }

  /**
   * 获取所有节点属性信息列表
   * @return 节点属性信息集合
   */
  public ArrayList<NodeAttributeInfo> getNodeAttributesInfo() {
    return nodeAttributesInfo;
  }
}