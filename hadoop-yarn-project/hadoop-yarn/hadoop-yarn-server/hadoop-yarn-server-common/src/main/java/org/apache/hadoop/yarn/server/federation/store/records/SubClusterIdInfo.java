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

package org.apache.hadoop.yarn.server.federation.store.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN联邦策略配置JSON格式中的子集群标识符封装类
 * 用于在序列化/反序列化策略配置时承载子集群ID信息
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@XmlRootElement(name = "federation-policy")
@XmlAccessorType(XmlAccessType.FIELD)
public class SubClusterIdInfo {

  private String id;

  /**
   * JAXB序列化所需的无参构造方法
   */
  public SubClusterIdInfo() {
    //JAXB needs this
  }

  /**
   * 从字符串ID构造子集群标识符信息对象
   * @param subClusterId 子集群ID字符串
   */
  public SubClusterIdInfo(String subClusterId) {
    this.id = subClusterId;
  }

  /**
   * 从SubClusterId对象构造子集群标识符信息对象
   * @param subClusterId 子集群ID对象
   */
  public SubClusterIdInfo(SubClusterId subClusterId) {
    this.id = subClusterId.getId();
  }

  /**
   * 将当前封装的字符串ID转换为SubClusterId对象
   * @return 转换后的子集群ID对象
   */
  @JsonProperty("id")
  public SubClusterId toId() {
    return SubClusterId.newInstance(id);
  }

  @Override
  public boolean equals(Object obj) {
    // 同一对象直接返回相等
    if (this == obj) {
      return true;
    }

    // 比较对象为null直接返回不相等
    if (obj == null) {
      return false;
    }

    // 类型相同则比较id字段，否则不相等
    if (obj instanceof SubClusterIdInfo) {
      SubClusterIdInfo other = (SubClusterIdInfo) obj;
      return new EqualsBuilder()
          .append(this.id, other.id)
          .isEquals();
    }

    return false;
  }

  @Override
  public int hashCode() {
    // 基于id字段计算哈希值
    return new HashCodeBuilder().append(this.id).toHashCode();
  }

  @Override
  public String toString() {
    // 直接返回id字符串
    return id;
  }
}