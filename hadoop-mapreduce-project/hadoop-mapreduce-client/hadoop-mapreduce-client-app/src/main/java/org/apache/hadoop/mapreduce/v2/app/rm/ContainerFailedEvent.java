// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE
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

/**
 * 容器分配失败事件，用于在MapReduce应用程序Master通知容器分配器任务尝试获取的容器分配失败
 * 承载了失败容器所属任务尝试标识和节点管理器地址信息，用于后续重新分配容器
 */
public class ContainerFailedEvent extends ContainerAllocatorEvent {

  // 失败容器所在节点管理器地址
  private final String contMgrAddress;
  
  /**
   * 构造容器分配失败事件
   * @param attemptID 失败容器对应的任务尝试标识
   * @param contMgrAddr 失败容器所在节点管理器地址
   */
  public ContainerFailedEvent(TaskAttemptId attemptID, String contMgrAddr) {
    super(attemptID, ContainerAllocator.EventType.CONTAINER_FAILED);
    this.contMgrAddress = contMgrAddr;
  }

  /**
   * 获取失败容器所在节点管理器地址
   * @return 节点管理器地址字符串
   */
  public String getContMgrAddress() {
    return contMgrAddress;
  }

}