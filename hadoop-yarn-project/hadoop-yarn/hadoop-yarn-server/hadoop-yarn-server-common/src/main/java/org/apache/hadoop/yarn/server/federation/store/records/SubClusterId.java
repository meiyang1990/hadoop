// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * 子集群唯一标识符，用于YARN联邦集群中标识参与联邦的子集群。
 * <p>
 * 全局唯一性由联邦初始化时的<code>FederationMembershipStateStore</code>保证。
 */
@Private
@Unstable
public abstract class SubClusterId implements Comparable<SubClusterId> {

  /**
   * 根据字符串ID创建新的子集群标识符实例。
   * @param subClusterId 子集群唯一标识字符串
   * @return 子集群标识符实例
   */
  @Private
  @Unstable
  public static SubClusterId newInstance(String subClusterId) {
    SubClusterId id = Records.newRecord(SubClusterId.class);
    id.setId(subClusterId);
    return id;
  }

  /**
   * 根据整数ID创建新的子集群标识符实例。
   * @param subClusterId 子集群唯一标识整数
   * @return 子集群标识符实例
   */
  @Private
  @Unstable
  public static SubClusterId newInstance(Integer subClusterId) {
    SubClusterId id = Records.newRecord(SubClusterId.class);
    id.setId(String.valueOf(subClusterId));
    return id;
  }

  /**
   * 获取子集群唯一标识字符串，该标识在整个联邦集群中唯一，并且会在重启和故障转移后保持不变。
   *
   * @return 子集群唯一标识符
   */
  @Public
  @Unstable
  public abstract String getId();

  /**
   * 设置子集群唯一标识字符串，该标识在整个联邦集群中唯一，并且会在重启和故障转移后保持不变。
   *
   * @param subClusterId 子集群唯一标识符
   */
  @Private
  @Unstable
  protected abstract void setId(String subClusterId);

  @Override
  public boolean equals(Object obj) {
    // 同一对象直接返回相等
    if (this == obj) {
      return true;
    }

    // 空对象直接返回不相等
    if (obj == null) {
      return false;
    }

    // 类型匹配则比较ID值
    if (obj instanceof SubClusterId) {
      SubClusterId other = (SubClusterId) obj;
      return new EqualsBuilder()
          .append(this.getId(), other.getId())
          .isEquals();
    }

    // 类型不匹配返回不相等
    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(this.getId()).
        toHashCode();
  }

  @Override
  public int compareTo(SubClusterId other) {
    // 按ID字符串字典序比较
    return getId().compareTo(other.getId());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getId());
    return sb.toString();
  }
}