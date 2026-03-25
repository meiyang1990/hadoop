// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * 封装可被抢占杀死的容器信息，用于容量调度器抢占机制。
 * 记录待kill容器、所属节点分区和叶子队列信息，为抢占决策提供元数据。
 */
public class KillableContainer {
  RMContainer container;
  String partition;
  String leafQueueName;

  /**
   * 构造可被抢占杀死的容器信息对象。
   * @param container 待处理的RM容器实例
   * @param partition 容器所在节点分区
   * @param leafQueueName 容器所属叶子队列名称
   */
  public KillableContainer(RMContainer container, String partition, String leafQueueName) {
    this.container = container;
    this.partition = partition;
    this.leafQueueName = leafQueueName;
  }

  public RMContainer getRMContainer() {
    return this.container;
  }

  public String getNodePartition() {
    return this.partition;
  }

  public String getLeafQueueName() {
    return this.leafQueueName;
  }
}