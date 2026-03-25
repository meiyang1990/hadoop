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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * 已分配GPU设备封装，扩展基础GPU设备信息，增加了容器分配相关运行时信息
 * 除了{@link GpuDevice}基础信息外，还包含了当前使用该GPU的容器ID等运行时信息
 */
public class AssignedGpuDevice extends GpuDevice {
  private static final long serialVersionUID = -12983712986315L;

  // 分配该GPU的容器ID字符串
  String containerId;

  /**
   * 构造已分配给指定容器的GPU设备对象
   * @param index GPU设备索引
   * @param minorNumber GPU设备minor号
   * @param containerId 分配该GPU的容器ID
   */
  public AssignedGpuDevice(int index, int minorNumber,
      ContainerId containerId) {
    super(index, minorNumber);
    this.containerId = containerId.toString();
  }

  /**
   * 空构造方法
   */
  public AssignedGpuDevice() {
  }

  /**
   * 获取分配该GPU的容器ID字符串
   * @return 容器ID字符串
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * 设置分配该GPU的容器ID
   * @param containerId 容器ID字符串
   */
  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AssignedGpuDevice)) {
      return false;
    }
    AssignedGpuDevice other = (AssignedGpuDevice) obj;
    return index == other.index && minorNumber == other.minorNumber
        && containerId.equals(other.containerId);
  }

  @Override
  public int compareTo(Object obj) {
    if ((!(obj instanceof AssignedGpuDevice))) {
      return -1;
    }

    AssignedGpuDevice other = (AssignedGpuDevice) obj;

    // 优先按索引比较
    int result = Integer.compare(index, other.index);
    if (0 != result) {
      return result;
    }
    // 索引相同按minor号比较
    result = Integer.compare(minorNumber, other.minorNumber);
    if (0 != result) {
      return result;
    }
    // 最后按容器ID比较
    return containerId.compareTo(other.containerId);
  }

  @Override
  public int hashCode() {
    final int prime = 47;
    return prime * (prime * index + minorNumber) + containerId.hashCode();
  }
}