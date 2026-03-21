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

package org.apache.hadoop.yarn.server.timelineservice.storage.application;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * YARN时间线服务存储应用信息的HBase表定义类，定义了应用表的结构。
 * 应用表包含三个列族：info存储应用基本信息、config存储应用配置数据、metrics存储应用指标数据。
 * 独立创建该表是为了提升查询性能，结构与实体表类似但专门用于应用数据存储。
 *
 * 示例应用表记录结构:
 *
 * <pre>
 * |-------------------------------------------------------------------------|
 * |  Row       | Column Family                | Column Family| Column Family|
 * |  Rowkey    | info                         | metrics      | config       |
 * |-------------------------------------------------------------------------|
 * | clusterId! | id:appId                     | metricId1:   | configKey1:  |
 * | userName!  |                              | metricValue1 | configValue1 |
 * | flowName!  | created_time:                | @timestamp1  |              |
 * | flowRunId! | 1392993084018                |              | configKey2:  |
 * | AppId      |                              | metriciD1:   | configValue2 |
 * |            | i!infoKey:                   | metricValue2 |              |
 * |            | infoValue                    | @timestamp2  |              |
 * |            |                              |              |              |
 * |            | r!relatesToKey:              | metricId2:   |              |
 * |            | id3=id4=id5                  | metricValue1 |              |
 * |            |                              | @timestamp2  |              |
 * |            | s!isRelatedToKey:            |              |              |
 * |            | id7=id9=id6                  |              |              |
 * |            |                              |              |              |
 * |            | e!eventId=timestamp=infoKey: |              |              |
 * |            | eventInfoValue               |              |              |
 * |            |                              |              |              |
 * |            | flowVersion:                 |              |              |
 * |            | versionValue                 |              |              |
 * |-------------------------------------------------------------------------|
 * </pre>
 */
/**
 * 应用信息HBase表实现类，继承自通用表基类，专门用于存储YARN应用时间线数据。
 */
public final class ApplicationTable extends BaseTable<ApplicationTable> {
}