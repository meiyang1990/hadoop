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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;

/**
 * YARN调度器可调度实体接口，定义所有可被调度单元需要实现的公共方法
 * 典型实现包括应用、应用尝试等需要分配资源的调度单元
 */
public interface SchedulableEntity {
  
  /**
   * 获取可调度实体唯一ID
   * @return 实体唯一ID
   */
  public String getId();
  
  /**
   * 按输入顺序比较两个可调度实体，用于先进先出排序
   * 具体排序规则由实现类定义
   *
   * @param other 待比较的另一个可调度实体
   * @return 比较结果，小于0表示当前实体在前，大于0表示另一个实体在前，等于0表示顺序相同
   */
  public int compareInputOrderTo(SchedulableEntity other);
  
  /**
   * 获取当前实体的资源使用情况视图，包含已分配和请求的资源
   * @return 资源使用对象
   */
  public ResourceUsage getSchedulingResourceUsage();
  
  /**
   * 获取当前应用的优先级
   * @return 应用优先级
   */
  public Priority getPriority();

  /**
   * 判断该实体是否是RM重启前正在运行、正在恢复的应用
   * @return true 表示应用正在恢复，false 表示是新启动的应用
   */
  public boolean isRecovering();

  /**
   * 获取该实体对应的节点分区（节点标签）
   * @return 分区节点标签
   */
  String getPartition();

  /**
   * 获取作业的启动时间戳
   * @return 启动时间戳（毫秒）
   */
  long getStartTime();
}