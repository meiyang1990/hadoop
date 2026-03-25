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

import org.apache.hadoop.yarn.api.records.ReservationDefinition;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN RM WebAPI 预约定义信息数据访问对象，用于封装预约定义信息并支持XML/JSON序列化
 * 供Web页面和REST API返回预约定义数据使用
 */
@XmlRootElement(name = "reservation-definition")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationDefinitionInfo {

  @XmlElement(name = "arrival")
  private long arrival;

  @XmlElement(name = "deadline")
  private long deadline;

  @XmlElement(name = "reservation-requests")
  private ReservationRequestsInfo reservationRequests;

  @XmlElement(name = "reservation-name")
  private String reservationName;

  @XmlElement(name = "priority")
  private int priority;

  @XmlElement(name = "recurrence-expression")
  private String recurrenceExpression;

  /**
   * 无参构造函数，供JAXB序列化框架使用
   */
  public ReservationDefinitionInfo() {

  }

  /**
   * 根据API层的ReservationDefinition构造Web层DTO对象
   * @param definition API层预约定义实例
   */
  public ReservationDefinitionInfo(ReservationDefinition definition) {
    arrival = definition.getArrival();
    deadline = definition.getDeadline();
    reservationName = definition.getReservationName();
    reservationRequests = new ReservationRequestsInfo(definition
            .getReservationRequests());
    recurrenceExpression = definition.getRecurrenceExpression();
  }

  /**
   * 获取预约开始时间
   * @return 预约开始时间戳
   */
  public long getArrival() {
    return arrival;
  }

  public void setArrival(long arrival) {
    this.arrival = arrival;
  }

  /**
   * 获取预约结束时间
   * @return 预约结束时间戳
   */
  public long getDeadline() {
    return deadline;
  }

  public void setDeadline(long deadline) {
    this.deadline = deadline;
  }

  /**
   * 获取预约资源请求信息
   * @return 预约资源请求DTO对象
   */
  public ReservationRequestsInfo getReservationRequests() {
    return reservationRequests;
  }

  public void setReservationRequests(
      ReservationRequestsInfo reservationRequests) {
    this.reservationRequests = reservationRequests;
  }

  /**
   * 获取预约名称
   * @return 预约名称字符串
   */
  public String getReservationName() {
    return reservationName;
  }

  public void setReservationName(String reservationName) {
    this.reservationName = reservationName;
  }

  /**
   * 获取预约优先级
   * @return 优先级数值
   */
  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  /**
   * 获取周期预约重复执行表达式
   * @return 周期表达式字符串
   */
  public String getRecurrenceExpression() {
    return recurrenceExpression;
  }

  public void setRecurrenceExpression(String recurrenceExpression) {
    this.recurrenceExpression = recurrenceExpression;
  }

}