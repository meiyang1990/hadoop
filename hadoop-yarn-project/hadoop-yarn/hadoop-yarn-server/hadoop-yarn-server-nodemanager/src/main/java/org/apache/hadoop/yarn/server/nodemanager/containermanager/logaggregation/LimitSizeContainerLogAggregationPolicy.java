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
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;

/**
 * 基于日志大小限制的容器日志聚合策略，仅对非日志超限被杀的容器执行日志聚合
 */
@Private
public class LimitSizeContainerLogAggregationPolicy extends
    AbstractContainerLogAggregationPolicy {
  /**
   * 判断是否需要对当前容器执行日志聚合
   * @param logContext 容器日志上下文，包含容器退出信息
   * @return true 需要聚合，false 不需要聚合
   */
  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
    // 仅当容器不是因为日志超限被杀死时，才执行日志聚合
    return logContext.getExitCode()
        != ContainerExitStatus.KILLED_FOR_EXCESS_LOGS;
  }
}