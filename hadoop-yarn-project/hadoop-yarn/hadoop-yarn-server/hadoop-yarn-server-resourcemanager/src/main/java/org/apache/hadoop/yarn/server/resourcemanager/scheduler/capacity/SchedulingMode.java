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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

/**
 * 容量调度器的节点分区调度模式枚举，定义不同分区资源分配策略
 */
public enum SchedulingMode {
  /**
   * <p>
   * 当节点存在分区（例如 partition=x）时，仅当队列有权访问分区x，且应用申请分区x资源，才能分配该节点资源。
   * </p>
   * 
   * <p>
   * 当节点无分区时，仅申请非分区资源的应用可以分配该节点资源。
   * </p>
   * 严格遵守分区排他性，不同分区的资源不能混用
   */
  RESPECT_PARTITION_EXCLUSIVITY,
  
  /**
   * 仅适用于节点存在分区、且分区不是排他分区，应用申请非分区资源的场景：
   * 忽略分区排他性，允许非分区资源使用空闲的分区节点资源
   */
  IGNORE_PARTITION_EXCLUSIVITY
}