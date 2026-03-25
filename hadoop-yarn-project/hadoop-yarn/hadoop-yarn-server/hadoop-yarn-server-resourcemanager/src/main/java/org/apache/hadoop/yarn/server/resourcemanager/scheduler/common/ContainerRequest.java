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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;

import java.util.List;

/**
 * 容器资源请求封装类，保存与容器关联的资源请求信息。
 * 主要用途：当容器在被ApplicationMaster获取前被抢占或取消时，调度器可恢复对应资源请求。
 * 容器分配后，此处保存的是已经扣除分配资源后的剩余请求。
 * 
 * 生命周期：
 * <pre>
 * 1) 创建容器时实例化本对象
 * 2) 由调度器设置到ContainerImpl对象中
 * 3) 容器被抢占/取消且未被AM获取时，本对象会被加回待处理请求池
 * 4) 容器被AM获取后，本对象会从ContainerImpl中清除
 * </pre>
 */
public class ContainerRequest {
  private List<ResourceRequest> requests;
  private SchedulingRequest schedulingRequest;

  /**
   * 基于传统资源请求列表构造容器请求对象
   * @param requests 传统资源请求列表
   */
  public ContainerRequest(List<ResourceRequest> requests) {
    this.requests = requests;
    schedulingRequest = null;
  }

  /**
   * 基于新版调度请求构造容器请求对象
   * @param schedulingRequest 调度请求对象
   */
  public ContainerRequest(SchedulingRequest schedulingRequest) {
    this.schedulingRequest = schedulingRequest;
    this.requests = null;
  }

  /**
   * 获取传统资源请求列表
   * @return 传统资源请求列表
   */
  public List<ResourceRequest> getResourceRequests() {
    return requests;
  }

  /**
   * 获取新版调度请求对象
   * @return 调度请求对象
   */
  public SchedulingRequest getSchedulingRequest() {
    return schedulingRequest;
  }
}