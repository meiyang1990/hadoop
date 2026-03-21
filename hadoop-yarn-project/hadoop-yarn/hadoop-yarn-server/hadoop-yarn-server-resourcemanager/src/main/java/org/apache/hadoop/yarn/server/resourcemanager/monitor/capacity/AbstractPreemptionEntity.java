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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;


import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;


/**
 * 容量调度抢占计算的抽象基类，用于跟踪队列或应用的资源可用性、待分配资源和当前利用率等临时计算数据
 */
public class AbstractPreemptionEntity {
  // 从调度器复制的基础信息，所属队列名称
  final String queueName;

  protected final Resource current;
  protected final Resource amUsed;
  protected final Resource reserved;
  protected Resource pending;

  // 抢占候选选择过程中使用的计算字段
  Resource idealAssigned;
  Resource toBePreempted;
  Resource selected;
  private Resource actuallyToBePreempted;
  private Resource toBePreemptFromOther;

  /**
   * 构造抢占计算实体，初始化从调度器复制的基础资源信息
   * @param queueName 所属队列名称
   * @param usedPerPartition 当前分区已用资源
   * @param amUsedPerPartition 当前分区ApplicationMaster占用资源
   * @param reserved 当前预留资源
   * @param pendingPerPartition 当前分区待分配资源
   */
  AbstractPreemptionEntity(String queueName, Resource usedPerPartition,
      Resource amUsedPerPartition, Resource reserved,
      Resource pendingPerPartition) {
    this.queueName = queueName;
    this.current = usedPerPartition;
    this.pending = pendingPerPartition;
    this.reserved = reserved;
    this.amUsed = amUsedPerPartition;

    this.idealAssigned = Resource.newInstance(0, 0);
    this.actuallyToBePreempted = Resource.newInstance(0, 0);
    this.toBePreempted = Resource.newInstance(0, 0);
    this.toBePreemptFromOther = Resource.newInstance(0, 0);
    this.selected = Resource.newInstance(0, 0);
  }

  public String getQueueName() {
    return queueName;
  }

  public Resource getUsed() {
    return current;
  }

  public Resource getUsedDeductAM() {
    return Resources.subtract(current, amUsed);
  }

  public Resource getAMUsed() {
    return amUsed;
  }

  public Resource getPending() {
    return pending;
  }

  public Resource getReserved() {
    return reserved;
  }

  public Resource getActuallyToBePreempted() {
    return actuallyToBePreempted;
  }

  public void setActuallyToBePreempted(Resource actuallyToBePreempted) {
    this.actuallyToBePreempted = actuallyToBePreempted;
  }

  public Resource getToBePreemptFromOther() {
    return toBePreemptFromOther;
  }

  public void setToBePreemptFromOther(Resource toBePreemptFromOther) {
    this.toBePreemptFromOther = toBePreemptFromOther;
  }

}