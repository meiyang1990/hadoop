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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;


import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * 容器分配过期信息，用于保存待处理的容器分配超时信息，支持排序和去重比较
 */
public class AllocationExpirationInfo implements
    Comparable<AllocationExpirationInfo> {

  private final ContainerId containerId;
  private final boolean increase;

  /**
   * 构造非扩容的容器分配过期信息
   * @param containerId 容器ID
   */
  public AllocationExpirationInfo(ContainerId containerId) {
    this(containerId, false);
  }

  /**
   * 构造完整的容器分配过期信息
   * @param containerId 容器ID
   * @param increase 是否是容器扩容分配
   */
  public AllocationExpirationInfo(
      ContainerId containerId, boolean increase) {
    this.containerId = containerId;
    this.increase = increase;
  }

  /**
   * 获取关联容器ID
   * @return 容器ID
   */
  public ContainerId getContainerId() {
    return this.containerId;
  }

  /**
   * 判断是否是容器扩容分配
   * @return true表示是扩容分配，false表示是普通分配
   */
  public boolean isIncrease() {
    return this.increase;
  }

  @Override
  public int hashCode() {
    // 仅基于容器ID计算哈希，移位减少冲突
    return (getContainerId().hashCode() << 16);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof AllocationExpirationInfo)) {
      return false;
    }
    // 通过比较容器ID判断相等
    return compareTo((AllocationExpirationInfo)other) == 0;
  }

  @Override
  public int compareTo(AllocationExpirationInfo other) {
    if (other == null) {
      return -1;
    }
    // 仅需要比较容器ID，同一容器的分配过期信息视为相等
    return getContainerId().compareTo(other.getContainerId());
  }

  @Override
  public String toString() {
    return "<container=" + getContainerId() + ", increase="
        + isIncrease() + ">";
  }
}