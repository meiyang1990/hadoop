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

import java.io.Serializable;

/**
 * YARN NodeManager GPU资源分配中代表单个GPU设备的实体类，存储GPU设备基本信息
 */
public class GpuDevice implements Serializable, Comparable {
  // GPU设备在当前节点的索引序号
  protected int index;
  // GPU设备的次设备号，Linux系统中用于标识设备文件
  protected int minorNumber;
  private static final long serialVersionUID = -6812314470754667710L;

  /**
   * 构造GPU设备对象
   * @param index 节点内GPU索引
   * @param minorNumber Linux系统GPU次设备号
   */
  public GpuDevice(int index, int minorNumber) {
    this.index = index;
    this.minorNumber = minorNumber;
  }

  public GpuDevice() {
  }

  /**
   * 获取GPU在当前节点的索引
   * @return 节点内GPU索引
   */
  public int getIndex() {
    return index;
  }

  /**
   * 获取GPU的Linux次设备号
   * @return 次设备号
   */
  public int getMinorNumber() {
    return minorNumber;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GpuDevice)) {
      return false;
    }
    GpuDevice other = (GpuDevice) obj;
    // 索引和次设备号都相同才判定为同一个GPU设备
    return index == other.index && minorNumber == other.minorNumber;
  }

  @Override
  public int compareTo(Object obj) {
    if (!(obj instanceof  GpuDevice)) {
      return -1;
    }

    GpuDevice other = (GpuDevice) obj;

    // 先按索引排序，索引相同再按次设备号排序
    int result = Integer.compare(index, other.index);
    if (0 != result) {
      return result;
    }
    return Integer.compare(minorNumber, other.minorNumber);
  }

  @Override
  public int hashCode() {
    final int prime = 47;
    // 基于索引和次设备号计算哈希值
    return prime * index + minorNumber;
  }

  @Override
  public String toString() {
    // 输出GPU设备信息字符串，用于日志和调试
    return "(index=" + index + ",minor_number=" + minorNumber + ")";
  }
}