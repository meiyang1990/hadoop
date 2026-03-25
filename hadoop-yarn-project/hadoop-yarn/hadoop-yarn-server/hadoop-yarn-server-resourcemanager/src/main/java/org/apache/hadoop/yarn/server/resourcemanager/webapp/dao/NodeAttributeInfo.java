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

import org.apache.hadoop.yarn.api.records.NodeAttribute;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 节点属性信息的数据访问对象，用于RM Web UI接口返回节点属性数据
 */
@XmlRootElement(name = "nodeAttributeInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeAttributeInfo {

  private String prefix;
  private String name;
  private String type;
  private String value;

  /**
   * JAXB反序列化需要的无参构造函数
   */
  public NodeAttributeInfo() {
    // JAXB needs this
  }

  /**
   * 根据NodeAttribute对象构造节点属性信息
   * @param nodeAttribute 节点属性源对象
   */
  public NodeAttributeInfo(NodeAttribute nodeAttribute) {
    this.prefix = nodeAttribute.getAttributeKey().getAttributePrefix();
    this.name = nodeAttribute.getAttributeKey().getAttributeName();
    this.type = nodeAttribute.getAttributeType().toString();
    this.value = nodeAttribute.getAttributeValue();
  }

  /** 获取属性前缀 */
  public String getPrefix() {
    return prefix;
  }

  /** 获取属性名称 */
  public String getName() {
    return name;
  }

  /** 获取属性类型 */
  public String getType() {
    return type;
  }

  /** 获取属性值 */
  public String getValue() {
    return value;
  }
}