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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * YARN资源调度器支持资源抢占接口，定义支持容器抢占/杀死的调度器契约
 * 属于YARN资源调度器扩展能力，允许高优先级任务抢占低优先级任务已分配资源
 */
public interface PreemptableResourceScheduler extends ResourceScheduler {

  /**
   *  删除指定已预留容器的预留记录，释放预留资源
   * 当调度器支持容器预留时使用，用于取消无效预留
   * @param container 已预留的容器对象引用
   */
  void killReservedContainer(RMContainer container);

  /**
   * 标记指定容器为待抢占状态，触发抢占流程回收容器资源
   * 用于将指定应用的容器标记为可抢占，供高优先级任务回收使用
   * @param aid 需要回收容器所属的应用尝试ID
   * @param container 待回收抢占的容器对象
   */
  void markContainerForPreemption(ApplicationAttemptId aid, RMContainer container);

  /**
   * 标记指定容器为可强制杀死状态，强制回收容器资源
   * 用于需要立即回收资源场景，直接强制终止容器
   * @param container 待强制杀死的RM容器对象
   */
  void markContainerForKillable(RMContainer container);

}