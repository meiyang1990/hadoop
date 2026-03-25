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
 * 容量调度器应用主人(AM)容器启动失败诊断信息常量定义
 * 存储各类AM容器分配跳过场景的诊断信息文本
 */
public interface CSAMContainerLaunchDiagnosticsConstants {
  // 忽略独占模式下跳过节点分配
  String SKIP_AM_ALLOCATION_IN_IGNORE_EXCLUSIVE_MODE =
      "Skipping assigning to Node in Ignore Exclusivity mode. ";
  // 节点被拉黑，跳过分配
  String SKIP_AM_ALLOCATION_IN_BLACK_LISTED_NODE =
      "Skipped scheduling for this Node as its black listed. ";
  // 数据局部性不匹配，跳过分配
  String SKIP_AM_ALLOCATION_DUE_TO_LOCALITY =
      "Skipping assigning to Node as request locality is not matching. ";
  // 队列AM资源配额超限，无法分配
  String QUEUE_AM_RESOURCE_LIMIT_EXCEED =
      "Queue's AM resource limit exceeded. ";
  // 用户AM资源配额超限，无法分配
  String USER_AM_RESOURCE_LIMIT_EXCEED = "User's AM resource limit exceeded. ";
  // 应用最后处理过的节点信息前缀
  String LAST_NODE_PROCESSED_MSG =
      " Last Node which was processed for the application : ";
  // 集群无可用资源，跳过AM分配
  String CLUSTER_RESOURCE_EMPTY =
      "Skipping AM assignment as cluster resource is empty. ";
}