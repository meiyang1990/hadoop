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
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.NodeLabel;

/**
 * YARN RM Web UI 节点标签信息数据访问对象，封装节点标签的基础信息用于REST接口返回
 */
@XmlRootElement(name = "nodeLabelInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeLabelInfo {

  private String name;
  private boolean exclusivity;
  private PartitionInfo partitionInfo;
  private Integer activeNMs;

  /**
   * JAXB反序列化需要的无参构造方法
   */
  public NodeLabelInfo() {
    // JAXB needs this
  }

  /**
   * 使用标签名称构造节点标签信息，默认启用独占性
   * @param name 节点标签名称
   */
  public NodeLabelInfo(String name) {
    this.name = name;
    this.exclusivity = true;
  }

  /**
   * 使用标签名称和独占性构造节点标签信息
   * @param name 节点标签名称
   * @param exclusivity 是否独占节点
   */
  public NodeLabelInfo(String name, boolean exclusivity) {
    this.name = name;
    this.exclusivity = exclusivity;
  }

  /**
   * 从API层的NodeLabel对象转换构造节点标签信息
   * @param label API层节点标签对象
   */
  public NodeLabelInfo(NodeLabel label) {
    this.name = label.getName();
    this.exclusivity = label.isExclusive();
  }

  /**
   * 从API层NodeLabel和分区信息构造节点标签信息
   * @param label API层节点标签对象
   * @param partitionInfo 分区信息
   */
  public NodeLabelInfo(NodeLabel label, PartitionInfo partitionInfo) {
    this(label);
    this.partitionInfo = partitionInfo;
  }

  /**
   * 获取节点标签名称
   * @return 节点标签名称
   */
  public String getName() {
    return name;
  }

  /**
   * 获取节点标签独占性标识
   * @return true表示独占节点，false表示可共享节点
   */
  public boolean getExclusivity() {
    return exclusivity;
  }

  /**
   * 获取关联分区信息
   * @return 分区信息对象
   */
  public PartitionInfo getPartitionInfo() {
    return partitionInfo;
  }

  /**
   * 获取当前激活的NM节点数量
   * @return 激活NM节点数
   */
  public Integer getActiveNMs() {
    return activeNMs;
  }

  public void setActiveNMs(Integer activeNMs) {
    this.activeNMs = activeNMs;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setExclusivity(boolean exclusivity) {
    this.exclusivity = exclusivity;
  }

  public void setPartitionInfo(PartitionInfo partitionInfo) {
    this.partitionInfo = partitionInfo;
  }

  @Override
  public boolean equals(Object obj) {
    // 同一对象直接返回相等
    if (this == obj) {
      return true;
    }
    // 比较对象为null直接不相等
    if (obj == null) {
      return false;
    }
    // 类型不同直接不相等
    if (getClass() != obj.getClass()) {
      return false;
    }
    NodeLabelInfo other = (NodeLabelInfo) obj;
    // 标签名称不同不相等
    if (!getName().equals(other.getName())) {
      return false;
    }
    // 独占性不同不相等
    if (getExclusivity() != other.getExclusivity()) {
      return false;
    }
    // 名称和独占性都相同则相等
    return true;
  }

  @Override
  public int hashCode() {
    // 基于名称和独占性计算哈希值
    return (getName().hashCode() << 16) + (getExclusivity() ? 1 : 0);
  }
}