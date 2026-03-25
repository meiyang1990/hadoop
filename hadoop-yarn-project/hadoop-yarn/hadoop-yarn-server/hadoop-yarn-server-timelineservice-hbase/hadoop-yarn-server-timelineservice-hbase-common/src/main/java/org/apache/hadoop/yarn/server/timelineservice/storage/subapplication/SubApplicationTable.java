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

package org.apache.hadoop.yarn.server.timelineservice.storage.subapplication;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * 子应用表定义，用于在HBase中存储YARN时间线服务的子应用实体数据。
 * 该表包含三个列族：
 * <ul>
 *   <li>info：存储时间线实体的基础信息</li>
 *   <li>config：存储时间线实体的配置数据</li>
 *   <li>metrics：存储时间线实体的指标数据</li>
 * </ul>
 * 行键格式为：subAppUserId!clusterId!entityType!idPrefix!entityId!userId，
 * 用于支持按维度高效查询子应用时间线数据。
 */
public final class SubApplicationTable extends BaseTable<SubApplicationTable> {
}