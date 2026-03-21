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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 预约更新请求信息数据访问对象，用于封装REST API中预约更新请求的请求体信息
 * 接收用户提交的现有YARN资源预约更新所需参数
 */
@XmlRootElement(name = "reservation-update-context")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationUpdateRequestInfo {

  // 待更新的预约ID
  @XmlElement(name = "reservation-id")
  private String reservationId;

  // 更新后的预约定义信息
  @XmlElement(name = "reservation-definition")
  private ReservationDefinitionInfo reservationDefinition;

  public ReservationUpdateRequestInfo() {
  }

  /**
   * 获取待更新的预约ID
   * @return 预约ID
   */
  public String getReservationId() {
    return reservationId;
  }

  /**
   * 设置待更新的预约ID
   * @param reservationId 预约ID
   */
  public void setReservationId(String reservationId) {
    this.reservationId = reservationId;
  }

  /**
   * 获取更新后的预约定义信息
   * @return 预约定义信息
   */
  public ReservationDefinitionInfo getReservationDefinition() {
    return reservationDefinition;
  }

  /**
   * 设置更新后的预约定义信息
   * @param reservationDefinition 预约定义信息
   */
  public void setReservationDefinition(
      ReservationDefinitionInfo reservationDefinition) {
    this.reservationDefinition = reservationDefinition;
  }

}