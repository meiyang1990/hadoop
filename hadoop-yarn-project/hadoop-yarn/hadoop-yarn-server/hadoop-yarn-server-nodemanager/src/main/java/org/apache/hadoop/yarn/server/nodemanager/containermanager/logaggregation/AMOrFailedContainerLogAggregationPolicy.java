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
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;

/**
 * 日志聚合策略：仅对ApplicationMaster或执行失败的容器执行日志聚合。
 * 用于NodeManager日志聚合，减少正常成功容器的日志聚合存储开销。
 */
@Private
public class AMOrFailedContainerLogAggregationPolicy extends
    AbstractContainerLogAggregationPolicy {
  
  /**
   * 判断是否需要对当前容器执行日志聚合。
   * @param logContext 容器日志上下文，包含容器类型和退出码信息
   * @return true表示需要聚合，false表示不需要聚合
   */
  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
    int exitCode = logContext.getExitCode();
    // 满足任意条件则聚合：1.容器是ApplicationMaster 2.容器退出码非0且不是被强制杀死/正常终止
    return logContext.getContainerType() == ContainerType.APPLICATION_MASTER ||
        (exitCode != 0 && exitCode != ExitCode.FORCE_KILLED.getExitCode()
        && exitCode != ExitCode.TERMINATED.getExitCode());
  }
}