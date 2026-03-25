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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao;

import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.AssignedDevice;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * 节点管理器Web服务中设备资源分配信息的包装类，用于REST API序列化返回设备资源信息
 * */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class NMDeviceResourceInfo extends NMResourceInfo {

  // 当前节点上的全部设备列表
  private List<Device> totalDevices;
  // 当前节点上已经分配出去的设备列表
  private List<AssignedDevice> assignedDevices;

  /**
   * 带参构造函数，初始化设备资源信息
   * @param totalDevices 节点全部设备列表
   * @param assignedDevices 已分配设备列表
   */
  public NMDeviceResourceInfo(
      List<Device> totalDevices, List<AssignedDevice> assignedDevices) {
    this.assignedDevices = assignedDevices;
    this.totalDevices = totalDevices;
  }

  /**
   * 默认无参构造函数，用于XML/JSON序列化
   */
  public NMDeviceResourceInfo() {
  }

  public List<Device> getTotalDevices() {
    return totalDevices;
  }

  public void setTotalDevices(List<Device> totalDevices) {
    this.totalDevices = totalDevices;
  }

  public List<AssignedDevice> getAssignedDevices() {
    return assignedDevices;
  }

  public void setAssignedDevices(
      List<AssignedDevice> assignedDevices) {
    this.assignedDevices = assignedDevices;
  }
}