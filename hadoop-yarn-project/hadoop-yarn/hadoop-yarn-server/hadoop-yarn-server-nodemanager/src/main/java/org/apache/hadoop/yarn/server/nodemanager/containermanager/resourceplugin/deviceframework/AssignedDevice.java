// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;

import java.io.Serializable;
import java.util.Objects;

/**
 * 已分配设备封装类，供NodeManager REST API对外展示设备分配信息使用
 * */
public class AssignedDevice implements Serializable, Comparable {

  private static final long serialVersionUID = -544285507952217366L;

  private Device device;
  private String containerId;

  /**
   * 空构造器，供序列化框架使用
   */
  public AssignedDevice() {
  }

  /**
   * 构造已分配设备对象，封装设备ID和所属容器ID
   * @param cId 分配设备的容器ID
   * @param dev 被分配的设备对象
   */
  public AssignedDevice(ContainerId cId, Device dev) {
    this.device = dev;
    this.containerId = cId.toString();
  }

  /**
   * 获取被分配的设备对象
   * @return 设备对象
   */
  public Device getDevice() {
    return device;
  }

  /**
   * 获取分配该设备的容器ID字符串
   * @return 容器ID字符串
   */
  public String getContainerId() {
    return containerId;
  }

  @Override
  public int compareTo(Object o) {
    if (!(o instanceof AssignedDevice)) {
      return -1;
    }
    AssignedDevice other = (AssignedDevice) o;
    // 先按设备比较
    int result = getDevice().compareTo(other.getDevice());
    if (0 != result) {
      return result;
    }
    // 设备相同则按容器ID比较
    return getContainerId().compareTo(other.getContainerId());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AssignedDevice)) {
      return false;
    }
    AssignedDevice other = (AssignedDevice) o;
    // 设备和容器ID都相同才判定为相等
    return getDevice().equals(other.getDevice())
        && getContainerId().equals(other.getContainerId());
  }

  @Override
  public int hashCode() {
    // 基于设备和容器ID计算哈希值
    return Objects.hash(getDevice(), getContainerId());
  }

}