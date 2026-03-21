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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.AssignedGpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

/**
 * 节点管理器GPU资源信息DAO，供NMWebServices获取节点资源信息时返回给客户端
 * Gpu device information return to client when
 * {@link org.apache.hadoop.yarn.server.nodemanager.webapp.NMWebServices#getNMResourceInfo(String)}
 * is invoked.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "NMGpuResourceInfo")
public class NMGpuResourceInfo extends NMResourceInfo {
  /** GPU设备整体信息，包含从节点获取的硬件详情 */
  GpuDeviceInformation gpuDeviceInformation;

  /** 当前节点上所有GPU设备列表 */
  List<GpuDevice> totalGpuDevices;
  /** 当前节点上已分配给容器的GPU设备列表 */
  List<AssignedGpuDevice> assignedGpuDevices;

  /**
   * 构造包含完整GPU信息的对象
   * @param gpuDeviceInformation GPU硬件整体信息
   * @param totalGpuDevices 节点全部GPU设备列表
   * @param assignedGpuDevices 节点已分配GPU设备列表
   */
  public NMGpuResourceInfo(GpuDeviceInformation gpuDeviceInformation,
      List<GpuDevice> totalGpuDevices,
      List<AssignedGpuDevice> assignedGpuDevices) {
    this.gpuDeviceInformation = gpuDeviceInformation;
    this.totalGpuDevices = totalGpuDevices;
    this.assignedGpuDevices = assignedGpuDevices;
  }

  /**
   * 无参构造函数，用于XML反序列化
   */
  public NMGpuResourceInfo() {
  }

  /**
   * 获取GPU硬件整体信息
   * @return GPU硬件整体信息对象
   */
  public GpuDeviceInformation getGpuDeviceInformation() {
    return gpuDeviceInformation;
  }

  /**
   * 设置GPU硬件整体信息
   * @param gpuDeviceInformation GPU硬件整体信息对象
   */
  public void setGpuDeviceInformation(
      GpuDeviceInformation gpuDeviceInformation) {
    this.gpuDeviceInformation = gpuDeviceInformation;
  }

  /**
   * 获取当前节点全部GPU设备列表
   * @return 全部GPU设备列表
   */
  public List<GpuDevice> getTotalGpuDevices() {
    return totalGpuDevices;
  }

  /**
   * 设置当前节点全部GPU设备列表
   * @param totalGpuDevices 全部GPU设备列表
   */
  public void setTotalGpuDevices(List<GpuDevice> totalGpuDevices) {
    this.totalGpuDevices = totalGpuDevices;
  }

  /**
   * 获取当前节点已分配GPU设备列表
   * @return 已分配GPU设备列表
   */
  public List<AssignedGpuDevice> getAssignedGpuDevices() {
    return assignedGpuDevices;
  }

  /**
   * 设置当前节点已分配GPU设备列表
   * @param assignedGpuDevices 已分配GPU设备列表
   */
  public void setAssignedGpuDevices(
      List<AssignedGpuDevice> assignedGpuDevices) {
    this.assignedGpuDevices = assignedGpuDevices;
  }
}