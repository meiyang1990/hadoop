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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * YARN资源调度约束求值接口，实现该接口的类表示可根据给定目标更新内部状态并完成求值。
 * 用于YARN调度过程中对 placement 分配约束进行评估计算。
 * @param <T> 求值所依赖的目标对象类型
 */
public interface Evaluable<T> {

  /**
   * 基于给定目标执行求值，该过程会修改当前对象的内部状态。
   * 在YARN调度约束流程中，用于根据目标节点/节点分区更新约束满足状态。
   *
   * @param target 影响求值结果的目标对象，通常为待评估的节点或节点分区
   * @throws YarnException 求值过程中发生错误时抛出
   */
  void evaluate(T target) throws YarnException;
}