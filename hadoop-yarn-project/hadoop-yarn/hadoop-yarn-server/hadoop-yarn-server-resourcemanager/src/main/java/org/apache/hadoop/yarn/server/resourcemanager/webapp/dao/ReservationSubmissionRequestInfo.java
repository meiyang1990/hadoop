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
 * YARN ResourceManager Web DAO类，封装资源预留提交请求的请求信息，
 * 用于接收REST API提交的资源预留请求，构造 ReservationSubmissionContext 后提交预留。
 */
@XmlRootElement(name = "reservation-submission-context")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationSubmissionRequestInfo {

  @XmlElement(name = "queue")
  private String queue;

  @XmlElement(name = "reservation-definition")
  private ReservationDefinitionInfo reservationDefinition;

  @XmlElement(name = "reservation-id")
  private String reservationId;

  /** 无参构造器，供JAXB序列化/反序列化使用 */
  public ReservationSubmissionRequestInfo() {
  }

  /** 获取提交目标队列名称 */
  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  /** 获取预留ID */
  public String getReservationId() {
    return reservationId;
  }

  public void setReservationId(String reservationId) {
    this.reservationId = reservationId;
  }

  /** 获取资源预留定义信息 */
  public ReservationDefinitionInfo getReservationDefinition() {
    return reservationDefinition;
  }

  public void setReservationDefinition(
      ReservationDefinitionInfo reservationDefinition) {
    this.reservationDefinition = reservationDefinition;
  }

}