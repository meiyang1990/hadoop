// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api;

import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

/**
 * 约束放置算法的标记接口，YARN调度器中用于容器节点放置约束的算法扩展点。
 * 唯一约定是需要使用RMContext进行初始化，便于算法获取调度器上下文信息。
 */
public interface ConstraintPlacementAlgorithm {

  /**
   * 初始化约束放置算法，传入资源管理器上下文供算法使用。
   * @param rmContext 资源管理器上下文对象，包含集群全局信息
   */
  void init(RMContext rmContext);

  /**
   * 根据输入约束计算容器的放置位置，并将结果输出到收集器中。
   * @param algorithmInput 算法输入，包含待放置容器和当前约束条件
   * @param collector 算法输出收集器，用于汇总放置结果
   */
  void place(ConstraintPlacementAlgorithmInput algorithmInput,
      ConstraintPlacementAlgorithmOutputCollector collector);
}