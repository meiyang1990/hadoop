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

package org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * AppFlow映射表定义，基于HBase存储应用ID到流信息的映射关系。
 * 核心存储应用ID对应流名称、流运行ID、用户ID的映射，用于时间线服务中
 * 快速通过应用ID查询所属流信息，支持多集群场景，不同集群的同一应用ID
 * 会分开存储为不同列。
 * 
 * 表结构示例：
 * <pre>
 * |--------------------------------------|
 * |  Row       | Column Family           |
 * |  key       | mapping                 |
 * |--------------------------------------|
 * | appId      | flow_name!cluster1:     |
 * |            | foo@daily_hive_report   |
 * |            |                         |
 * |            | flow_run_id!cluster1:   |
 * |            | 1452828720457           |
 * |            |                         |
 * |            | user_id!cluster1:       |
 * |            | admin                   |
 * |            |                         |
 * |            | flow_name!cluster2:     |
 * |            | bar@ad_hoc_query        |
 * |            |                         |
 * |            | flow_run_id!cluster2:   |
 * |            | 1452828498752           |
 * |            |                         |
 * |            | user_id!cluster2:       |
 * |            | joe                     |
 * |            |                         |
 * |--------------------------------------|
 * </pre>
 *
 * 在多集群环境中，虽然概率很低，但同一个应用ID可能出现在多个集群中，
 * 因此不同集群的信息会分别存储为不同的列。
 */
public final class AppToFlowTable extends BaseTable<AppToFlowTable> {
}