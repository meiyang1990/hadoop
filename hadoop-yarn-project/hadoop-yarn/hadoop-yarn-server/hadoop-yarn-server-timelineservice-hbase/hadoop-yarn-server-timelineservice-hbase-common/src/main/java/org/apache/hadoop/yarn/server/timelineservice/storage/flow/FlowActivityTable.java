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
 * 流活动HBase表定义，用于存储流每日活动记录，支持快速查询指定日期运行的流。
 * 该表可以快速检索某一天内有哪些流在运行，常用于时间线服务的流活动查询。
 * <p>
 * 表结构示例：
 * <pre>
 * |-------------------------------------------|
 * |  Row key   | Column Family                |
 * |            | info                         |
 * |-------------------------------------------|
 * | clusterId! | r!runid1:version1            |
 * | inv Top of |                              |
 * | Day!       | r!runid2:version7            |
 * | userName!  |                              |
 * | flowName   |                              |
 * |-------------------------------------------|
 * </pre>
 */
/**
 * 流活动表实现，继承自通用表基类，定义了YARN时间线服务中流活动存储结构。
 */
public final class FlowActivityTable extends BaseTable<FlowActivityTable> {
}