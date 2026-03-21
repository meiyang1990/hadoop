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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 资源分配信息数据访问对象，代表预约系统中一段时间内的资源分配信息，供Web API序列化返回
 */
@XmlRootElement(name = "resource-allocation")
@XmlAccessorType(XmlAccessType.FIELD)
public class ResourceAllocationInfo {
  private ResourceInfo resource;
  private long startTime;
  private long endTime;

  /**
   * 默认构造函数，初始化空的资源分配信息
   */
  public ResourceAllocationInfo() {
    resource = new ResourceInfo();
    startTime = -1;
    endTime = -1;
  }

  /**
   * 根据预约区间和资源构造资源分配信息对象
   * @param interval 预约时间区间
   * @param res 分配的资源
   */
  public ResourceAllocationInfo(ReservationInterval interval, Resource res) {
    this.resource = new ResourceInfo(res);
    this.startTime = interval.getStartTime();
    this.endTime = interval.getEndTime();
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long newStartTime) {
    this.startTime = newStartTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long newEndTime) {
    this.endTime = newEndTime;
  }

  public ResourceInfo getResource() {
    return resource;
  }

  public void setResource(ResourceInfo newResource) {
    this.resource = newResource;
  }
}