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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

/**
 * 容器启动数据记录，保存RM容器启动时确定的、需要持久化存储的核心信息，用于应用历史服务存储查询
 */
@Public
@Unstable
public abstract class ContainerStartData {

  /**
   * 创建新的容器启动数据实例，初始化所有必填字段
   * @param containerId 容器ID
   * @param allocatedResource 分配给容器的资源
   * @param assignedNode 容器分配到的节点ID
   * @param priority 容器调度优先级
   * @param startTime 容器启动时间戳
   * @return 初始化完成的容器启动数据实例
   */
  @Public
  @Unstable
  public static ContainerStartData newInstance(ContainerId containerId,
      Resource allocatedResource, NodeId assignedNode, Priority priority,
      long startTime) {
    ContainerStartData containerSD =
        Records.newRecord(ContainerStartData.class);
    containerSD.setContainerId(containerId);
    containerSD.setAllocatedResource(allocatedResource);
    containerSD.setAssignedNode(assignedNode);
    containerSD.setPriority(priority);
    containerSD.setStartTime(startTime);
    return containerSD;
  }

  @Public
  @Unstable
  public abstract ContainerId getContainerId();

  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  @Public
  @Unstable
  public abstract Resource getAllocatedResource();

  @Public
  @Unstable
  public abstract void setAllocatedResource(Resource resource);

  @Public
  @Unstable
  public abstract NodeId getAssignedNode();

  @Public
  @Unstable
  public abstract void setAssignedNode(NodeId nodeId);

  @Public
  @Unstable
  public abstract Priority getPriority();

  @Public
  @Unstable
  public abstract void setPriority(Priority priority);

  @Public
  @Unstable
  public abstract long getStartTime();

  @Public
  @Unstable
  public abstract void setStartTime(long startTime);

}