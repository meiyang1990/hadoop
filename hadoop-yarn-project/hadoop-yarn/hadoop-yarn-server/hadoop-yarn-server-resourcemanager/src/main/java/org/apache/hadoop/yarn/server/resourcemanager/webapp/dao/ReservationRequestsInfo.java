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

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequests;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 文件级注释：YARN ResourceManager Web REST API 数据访问对象，用于封装预约资源请求列表及其解释规则信息
 * 
 * 表示一组预约资源请求，同时包含解释规则（满足所有/任意/按顺序满足）语义信息，
 * 用于在Web API中序列化和传输预约定义信息。
 */
@XmlRootElement(name = "reservation-definition")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationRequestsInfo {

  // 预约请求解释规则的枚举序号，对应all/any/order三种语义
  @XmlElement(name = "reservation-request-interpreter")
  private int reservationRequestsInterpreter;
  // 转换后的单个预约请求信息列表
  @XmlElement(name = "reservation-request")
  private ArrayList<ReservationRequestInfo> reservationRequest;

  /**
   * JAXB反序列化默认构造函数
   */
  public ReservationRequestsInfo() {

  }

  /**
   * 基于原生ReservationRequests对象构造Web DAO对象
   * @param requests 原生预约请求对象，来自YARN API层
   */
  public ReservationRequestsInfo(ReservationRequests requests) {
    reservationRequest = new ArrayList<>();
    // 遍历所有资源请求，逐个转换为Web DAO格式
    for (ReservationRequest request : requests.getReservationResources()) {
      reservationRequest.add(new ReservationRequestInfo(request));
    }
    // 获取解释规则枚举的序号存储
    reservationRequestsInterpreter = requests.getInterpreter().ordinal();
  }

  public int getReservationRequestsInterpreter() {
    return reservationRequestsInterpreter;
  }

  public void setReservationRequestsInterpreter(
      int reservationRequestsInterpreter) {
    this.reservationRequestsInterpreter = reservationRequestsInterpreter;
  }

  public ArrayList<ReservationRequestInfo> getReservationRequest() {
    return reservationRequest;
  }

  public void setReservationRequest(
      ArrayList<ReservationRequestInfo> reservationRequest) {
    this.reservationRequest = reservationRequest;
  }

}