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

/**
 * YARN 约束放置算法输出收集器接口，用于收集约束放置算法执行产生的输出结果。
 * 是约束 placements 算法框架中，算法与结果处理模块之间的抽象接口。
 */
public interface ConstraintPlacementAlgorithmOutputCollector {

  /**
   * 收集约束放置算法的输出结果。
   * @param algorithmOutput 约束放置算法产生的输出结果对象
   */
  void collect(ConstraintPlacementAlgorithmOutput algorithmOutput);
}