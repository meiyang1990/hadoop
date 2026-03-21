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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 定义NodeManager上机会容器的排队策略，
 * 用于决定接受、排队还是拒绝机会容器的运行请求。
 */
public enum OpportunisticContainersQueuePolicy {
  /**
   * 根据队列容量限制决定是否排队容器，
   * 容量限制由配置项{@link YarnConfiguration#NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH}指定。
   * 如果队列还有剩余容量则将容器加入排队，否则拒绝该请求。
   */
  BY_QUEUE_LEN,
  /**
   * 根据节点剩余资源容量决定是否排队容器。
   * 汇总节点上正在运行和已经排队的资源总量，与节点总容量比较，
   * 仅当现有资源加上当前容器所需资源不超过节点总容量时，才接受该容器排队。
   */
  BY_RESOURCES;

  /** 默认排队策略，使用按队列长度限制 */
  public static final OpportunisticContainersQueuePolicy DEFAULT = BY_QUEUE_LEN;
}