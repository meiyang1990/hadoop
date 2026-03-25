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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * 容器请求事件，封装MapReduce任务尝试向YARN RM申请容器的所有请求信息
 * 用于在ApplicationMaster的容器分配器中传递容器分配请求
 */
public class ContainerRequestEvent extends ContainerAllocatorEvent {
  
  // 请求容器需要满足的资源能力
  private final Resource capability;
  // 数据本地性要求的主机列表
  private final String[] hosts;
  // 数据本地性要求的机架列表
  private final String[] racks;
  // 标记本次请求是否是由于之前尝试分配容器失败后发起的重试请求
  private boolean earlierAttemptFailed = false;

  /**
   * 构造完整参数的容器请求事件
   * @param attemptID 发起请求的任务尝试ID
   * @param capability 请求容器所需的资源大小
   * @param hosts 数据本地性要求的主机列表
   * @param racks 数据本地性要求的机架列表
   */
  public ContainerRequestEvent(TaskAttemptId attemptID, 
      Resource capability,
      String[] hosts, String[] racks) {
    super(attemptID, ContainerAllocator.EventType.CONTAINER_REQ);
    this.capability = capability;
    this.hosts = hosts;
    this.racks = racks;
  }
  
  /**
   * 私有构造方法，用于构造失败容器重试请求
   * @param attemptID 发起请求的任务尝试ID
   * @param capability 请求容器所需的资源大小
   */
  ContainerRequestEvent(TaskAttemptId attemptID, Resource capability) {
    this(attemptID, capability, new String[0], new String[0]);
    this.earlierAttemptFailed = true;
  }
  
  /**
   * 创建用于失败容器重试的容器请求事件
   * @param attemptID 发起请求的任务尝试ID
   * @param capability 请求容器所需的资源大小
   * @return 失败容器重试请求事件实例
   */
  public static ContainerRequestEvent createContainerRequestEventForFailedContainer(
      TaskAttemptId attemptID, 
      Resource capability) {
    //ContainerRequest for failed events does not consider rack / node locality?
    return new ContainerRequestEvent(attemptID, capability);
  }

  /**
   * 获取请求容器所需的资源能力
   * @return 资源描述对象
   */
  public Resource getCapability() {
    return capability;
  }

  /**
   * 获取数据本地性要求的主机列表
   * @return 主机名数组
   */
  public String[] getHosts() {
    return hosts;
  }
  
  /**
   * 获取数据本地性要求的机架列表
   * @return 机架名数组
   */
  public String[] getRacks() {
    return racks;
  }
  
  /**
   * 获取是否为之前请求失败后重试的标记
   * @return true表示本次请求是重试请求，false表示是首次请求
   */
  public boolean getEarlierAttemptFailed() {
    return earlierAttemptFailed;
  }
}