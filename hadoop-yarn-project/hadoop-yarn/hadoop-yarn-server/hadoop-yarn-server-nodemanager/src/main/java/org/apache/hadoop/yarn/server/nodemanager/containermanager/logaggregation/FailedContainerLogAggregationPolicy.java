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
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;

/**
 * 失败容器日志聚合策略，仅对异常失败的容器执行日志聚合。
 * 只对非零退出且不是被强制杀死/正常终止的容器聚合日志。
 */
@Private
public class FailedContainerLogAggregationPolicy extends
    AbstractContainerLogAggregationPolicy {
  /**
   * 判断是否需要对当前容器执行日志聚合。
   * @param logContext 容器日志上下文，包含容器退出码信息
   * @return true 需要聚合日志，false 不需要聚合
   */
  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
    int exitCode = logContext.getExitCode();
    return exitCode != 0 && exitCode != ExitCode.FORCE_KILLED.getExitCode()
        && exitCode != ExitCode.TERMINATED.getExitCode();
  }
}