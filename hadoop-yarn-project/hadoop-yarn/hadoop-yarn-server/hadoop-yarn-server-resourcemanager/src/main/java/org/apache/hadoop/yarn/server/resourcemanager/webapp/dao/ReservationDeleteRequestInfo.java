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
 * YARN ResourceManager Web REST API 数据传输对象，封装删除预约资源的请求信息，通过预约ID定位待删除预约。
 */
@XmlRootElement(name = "reservation-delete-context")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationDeleteRequestInfo {

  @XmlElement(name = "reservation-id")
  // 待删除预约的唯一标识ID
  private String reservationId;

  /**
   * 无参构造函数，供JAXB序列化框架使用。
   */
  public ReservationDeleteRequestInfo() {
    reservationId = null;
  }

  /**
   * 获取待删除预约的ID。
   * @return 预约ID字符串
   */
  public String getReservationId() {
    return reservationId;
  }

  /**
   * 设置待删除预约的ID。
   * @param reservationId 待删除的预约ID
   */
  public void setReservationId(String reservationId) {
    this.reservationId = reservationId;
  }

}