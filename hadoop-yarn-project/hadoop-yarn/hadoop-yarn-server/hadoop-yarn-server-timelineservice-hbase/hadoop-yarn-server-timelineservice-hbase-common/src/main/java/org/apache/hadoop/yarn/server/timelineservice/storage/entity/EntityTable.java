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

package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * 时间线服务实体表定义，基于HBase存储时间线实体数据。
 * 包含三个列族：info存储实体基础信息、config存储实体配置数据、metrics存储实体指标数据。
 * 
 * 实体表记录示例:
 *
 * <pre>
 * |-------------------------------------------------------------------------|
 * |  Row       | Column Family                | Column Family| Column Family|
 * |  key       | info                         | metrics      | config       |
 * |-------------------------------------------------------------------------|
 * | userName!  | id:entityId                  | metricId1:   | configKey1:  |
 * | clusterId! |                              | metricValue1 | configValue1 |
 * | flowName!  | type:entityType              | @timestamp1  |              |
 * | flowRunId! |                              |              | configKey2:  |
 * | AppId!     | created_time:                | metricId1:   | configValue2 |
 * | entityType!| 1392993084018                | metricValue2 |              |
 * | idPrefix!  |                              |              |              |
 * | entityId   | i!infoKey:                   |              |              |
 * |            | infoValue                    | metricId1:   |              |
 * |            |                              | metricValue1 |              |
 * |            | r!relatesToKey:              | @timestamp2  |              |
 * |            | id3=id4=id5                  |              |              |
 * |            |                              |              |              |
 * |            | s!isRelatedToKey             |              |              |
 * |            | id7=id9=id6                  |              |              |
 * |            |                              |              |              |
 * |            | e!eventId=timestamp=infoKey: |              |              |
 * |            | eventInfoValue               |              |              |
 * |            |                              |              |              |
 * |            | flowVersion:                 |              |              |
 * |            | versionValue                 |              |              |
 * |-------------------------------------------------------------------------|
 * </pre>
 * 
 * 继承自BaseTable，使用自身类型作为泛型参数，遵循HBase表定义的模式。
 */
public final class EntityTable extends BaseTable<EntityTable> {
}