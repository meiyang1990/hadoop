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

import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.ResourceAllocationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * YARN ResourceManager Web REST API 数据对象，封装资源预留信息，供WebUI展示使用
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationInfo {

  @XmlElement(name = "acceptance-time")
  private long acceptanceTime;

  private String user;

  @XmlElement(name = "resource-allocations")
  private List<ResourceAllocationInfo> resourceAllocations = new ArrayList<>();

  @XmlElement(name = "reservation-id")
  private String reservationId;

  @XmlElement(name = "reservation-definition")
  private ReservationDefinitionInfo reservationDefinition;

  /**
   * 无参构造函数，初始化默认值
   */
  public ReservationInfo() {
    acceptanceTime = 0;
    user = "";

    reservationDefinition = new ReservationDefinitionInfo();
  }

  /**
   * 根据资源预留分配状态构造资源预留信息对象
   * @param allocation 资源预留分配状态
   * @param includeResourceAllocations 是否包含资源分配详情
   * @throws Exception 构造过程异常
   */
  public ReservationInfo(ReservationAllocationState allocation, boolean
        includeResourceAllocations) throws
        Exception {
    // 提取接受时间
    acceptanceTime = allocation.getAcceptanceTime();
    // 提取提交用户
    user = allocation.getUser();

    // 如果需要包含资源分配详情，遍历所有请求转换信息
    if (includeResourceAllocations) {
      List<ResourceAllocationRequest> requests = allocation
              .getResourceAllocationRequests();
      for (ResourceAllocationRequest request : requests) {
        resourceAllocations.add(new ResourceAllocationInfo(new
                ReservationInterval(request.getStartTime(), request
                .getEndTime()), request.getCapability()));
      }
    }

    // 转换预留ID和预留定义信息
    reservationId = allocation.getReservationId().toString();
    reservationDefinition = new ReservationDefinitionInfo(
            allocation.getReservationDefinition());
  }

  public String getUser() {
    return user;
  }

  public void setUser(String newUser) {
    user = newUser;
  }

  public long getAcceptanceTime() {
    return acceptanceTime;
  }

  public List<ResourceAllocationInfo> getResourceAllocations() {
    return resourceAllocations;
  }

  public String getReservationId() {
    return reservationId;
  }

  public ReservationDefinitionInfo getReservationDefinition() {
    return reservationDefinition;
  }
}