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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.Container;

import java.util.List;

/**
 * 分布式调度模式下ApplicationMaster向ResourceManager发送的分配请求类
 * <p>
 * 用于将保障容器的分配请求转发给ResourceManager，同时通知ResourceManager节点
 * 分布式调度器已分配的机会容器信息。
 * </p>
 */
@Public
@Evolving
public abstract class DistributedSchedulingAllocateRequest {

  /**
   * 获取基础的AllocateRequest对象。
   * @return 容器分配请求
   */
  @Public
  @Evolving
  public abstract AllocateRequest getAllocateRequest();

  /**
   * 设置基础的AllocateRequest对象。
   * @param allocateRequest 容器分配请求
   */
  @Public
  @Evolving
  public abstract void  setAllocateRequest(AllocateRequest allocateRequest);

  /**
   * 获取NodeManager上分布式调度组件新分配的容器列表。
   * @return 新分配的容器列表
   */
  @Public
  @Evolving
  public abstract List<Container> getAllocatedContainers();

  /**
   * 设置NodeManager上分布式调度组件新分配的容器列表。
   * @param containers 新分配的容器列表
   */
  @Public
  @Evolving
  public abstract void setAllocatedContainers(List<Container> containers);
}