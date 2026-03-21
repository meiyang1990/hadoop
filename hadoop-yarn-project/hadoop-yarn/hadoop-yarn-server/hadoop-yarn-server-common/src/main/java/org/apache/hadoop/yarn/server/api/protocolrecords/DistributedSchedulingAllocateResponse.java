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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * 分布式调度模式下，ResourceManager对{@link DistributedSchedulingAllocateRequest}的响应类。
 * 包含ResourceManager分配的保障容器对应的AllocateResponse，以及可供分布式调度器分配容器使用的节点列表。
 */
@Public
@Unstable
public abstract class DistributedSchedulingAllocateResponse {

  /**
   * 创建仅包含AllocateResponse的DistributedSchedulingAllocateResponse新实例。
   * @param allResp ResourceManager分配响应
   * @return 响应实例
   */
  @Public
  @Unstable
  public static DistributedSchedulingAllocateResponse newInstance(
      AllocateResponse allResp) {
    DistributedSchedulingAllocateResponse response =
        Records.newRecord(DistributedSchedulingAllocateResponse.class);
    response.setAllocateResponse(allResp);
    return  response;
  }

  /**
   * 设置ResourceManager的分配响应。
   * @param response 分配响应
   */
  @Public
  @Unstable
  public abstract void setAllocateResponse(AllocateResponse response);

  /**
   * 获取ResourceManager的分配响应。
   * @return 分配响应
   */
  @Public
  @Unstable
  public abstract AllocateResponse getAllocateResponse();

  /**
   * 设置可供分布式调度使用的远程节点列表。
   * @param nodesForScheduling 可调度节点列表
   */
  @Public
  @Unstable
  public abstract void setNodesForScheduling(
      List<RemoteNode> nodesForScheduling);

  /**
   * 获取可供分布式调度使用的远程节点列表。
   * @return 可调度节点列表
   */
  @Public
  @Unstable
  public abstract List<RemoteNode> getNodesForScheduling();
}