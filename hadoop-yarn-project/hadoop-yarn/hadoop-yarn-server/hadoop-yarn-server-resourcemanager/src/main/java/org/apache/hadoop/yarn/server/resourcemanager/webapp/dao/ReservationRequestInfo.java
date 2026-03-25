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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN资源预留请求信息的数据访问对象，用于Web API序列化/反序列化
 * 封装资源预留请求的各项参数，供REST接口返回或接收请求数据
 */
@XmlRootElement(name = "reservation-definition")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationRequestInfo {

  // 预留资源的能力信息（包含内存、CPU等资源量）
  @XmlElement(name = "capability")
  private ResourceInfo capability;
  // 最小并发容器数量要求
  @XmlElement(name = "min-concurrency")
  private int minConcurrency;
  // 需要的总容器数量
  @XmlElement(name = "num-containers")
  private int numContainers;
  // 预留时长，单位毫秒
  @XmlElement(name = "duration")
  private long duration;

  /**
   * 默认构造函数，供JAXB序列化使用
   */
  public ReservationRequestInfo() {

  }

  /**
   * 根据API层的ReservationRequest构造DAO对象
   * @param request 原始资源预留请求对象
   */
  public ReservationRequestInfo(ReservationRequest request) {
    capability = new ResourceInfo(request.getCapability());
    minConcurrency = request.getConcurrency();
    duration = request.getDuration();
    numContainers = request.getNumContainers();
  }

  public ResourceInfo getCapability() {
    return capability;
  }

  public void setCapability(ResourceInfo capability) {
    this.capability = capability;
  }

  public int getMinConcurrency() {
    return minConcurrency;
  }

  public void setMinConcurrency(int minConcurrency) {
    this.minConcurrency = minConcurrency;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public void setNumContainers(int numContainers) {
    this.numContainers = numContainers;
  }

  public long getDuration() {
    return duration;
  }

  public void setDuration(long duration) {
    this.duration = duration;
  }

}