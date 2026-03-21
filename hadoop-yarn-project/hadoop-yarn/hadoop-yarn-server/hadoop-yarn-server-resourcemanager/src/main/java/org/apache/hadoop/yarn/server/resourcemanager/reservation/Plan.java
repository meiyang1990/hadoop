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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;

/**
 * 预留计划是YARN资源预留系统的核心数据结构，维护整个集群资源预留的"日程安排"，记录已接受的预留请求将如何分配资源。
 * <p>
 * 用户通过ResourceManager公共API提交的预留定义会传给对应的预留代理，预留代理会通过PlanView接口查询计划，
 * 判断当前计划中是否有足够资源满足该预留定义的时间和资源约束。如果找到可行分配，代理会通过PlanEdit接口
 * 将分配结果写入计划。成功后系统会向用户返回确认响应和预留ID，供后续访问预留资源使用。
 * <p>
 * PlanFollower会持续从计划读取数据，将当前时间片的资源分配发布给底层调度器，从而影响集群中运行作业的实时资源分配。
 * 例如会修改调度器的队列配置和权重，反映当前计划中的资源分配情况。
 * <p>
 * 该接口将方法按职责拆分为三个子接口：
 * <ul>
 * <li>{@link PlanContext}: 包含配置类信息</li>
 * <li>{@link PlanView}: 计划状态的只读访问</li>
 * <li>{@link PlanEdit}: 计划状态的写访问</li>
 * </ul>
 */
public interface Plan extends PlanContext, PlanView, PlanEdit {

}