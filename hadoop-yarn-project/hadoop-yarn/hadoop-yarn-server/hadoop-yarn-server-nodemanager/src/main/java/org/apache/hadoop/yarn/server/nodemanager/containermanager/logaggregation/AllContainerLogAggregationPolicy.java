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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;

/**
 * 全容器日志聚合策略，始终对所有容器执行日志聚合
 * 继承自AbstractContainerLogAggregationPolicy，满足强制聚合所有日志的场景需求
 */
@Private
public class AllContainerLogAggregationPolicy extends
    AbstractContainerLogAggregationPolicy {
  
  /**
   * 判断是否需要对指定容器执行日志聚合
   * @param logContext 容器日志上下文，包含容器相关信息
   * @return 始终返回true，所有容器都需要聚合日志
   */
  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
    return true;
  }
}