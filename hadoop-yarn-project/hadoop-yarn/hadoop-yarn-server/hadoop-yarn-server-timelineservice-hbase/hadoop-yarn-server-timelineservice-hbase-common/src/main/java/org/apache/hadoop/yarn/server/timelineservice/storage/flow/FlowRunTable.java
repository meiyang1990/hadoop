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

package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * 流程运行表(HBase)定义，用于存储聚合了多个应用的单个流程运行实例的元数据和指标信息
 * <p>
 * 核心存储内容：
 * <ul>
 *   <li>流程版本信息</li>
 *   <li>当前运行的应用数量</li>
 *   <li>流程的最早启动时间和最晚结束时间</li>
 *   <li>关联的应用ID列表</li>
 *   <li>全流程聚合后的指标数据</li>
 * </ul>
 * 行键格式为：clusterId!userName!flowName!flowRunId
 * 仅使用info一个列族存储所有数据
 * </p>
 */
public final class FlowRunTable extends BaseTable<FlowRunTable> {
}