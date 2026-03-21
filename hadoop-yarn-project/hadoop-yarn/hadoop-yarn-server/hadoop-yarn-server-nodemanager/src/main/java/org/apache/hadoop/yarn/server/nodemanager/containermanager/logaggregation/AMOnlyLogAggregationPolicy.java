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

/**
 * 仅对ApplicationMaster聚合日志的策略
 * 仅聚合应用程序主容器（AM）的日志，不聚合普通任务容器的日志
 */
@Private
public class AMOnlyLogAggregationPolicy extends
    AbstractContainerLogAggregationPolicy {
  
  /**
   * 判断是否需要对指定容器进行日志聚合
   * @param logContext 容器日志上下文，包含容器类型信息
   * @return 如果容器是ApplicationMaster则返回true，否则返回false
   */
  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
   return logContext.getContainerType() == ContainerType.APPLICATION_MASTER;
  }
}