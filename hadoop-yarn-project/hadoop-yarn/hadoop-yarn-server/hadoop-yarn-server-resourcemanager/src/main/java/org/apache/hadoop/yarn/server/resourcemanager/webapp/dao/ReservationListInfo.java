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

import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * YARN RM Web API 预约资源列表的数据传输对象，用于封装预约列表信息返回给前端。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationListInfo {
  @XmlElement(name = "reservations")
  // 存储预约信息列表
  private List<ReservationInfo> reservations;

  /**
   * 默认构造方法，初始化空预约列表。
   */
  public ReservationListInfo() {
    reservations = new ArrayList<>();
  }

  /**
   * 根据预约列表响应构造预约列表信息对象。
   * @param response 预约列表查询响应
   * @param includeResourceAllocations 是否包含资源分配信息
   * @throws Exception 构造过程异常
   */
  public ReservationListInfo(ReservationListResponse response,
        boolean includeResourceAllocations) throws Exception {
    this();

    // 遍历所有预约分配状态，转换为WebDAO格式
    for (ReservationAllocationState allocation :
            response.getReservationAllocationState()) {
      reservations.add(new ReservationInfo(allocation,
              includeResourceAllocations));
    }
  }

  /**
   * 获取预约信息列表。
   * @return 预约信息列表
   */
  public List<ReservationInfo> getReservations() {
    return reservations;
  }
}