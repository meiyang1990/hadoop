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

package org.apache.hadoop.yarn.server.timelineservice.storage.domain;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;

/**
 * 存储时间线服务域(Domain)信息的HBase表实现，用于管理时间线域的元数据信息（包括创建时间、描述、权限等）。
 * 域表结构：
 * <ul>
 *   <li>RowKey：由集群ID + 域ID拼接而成</li>
 *   <li>列族：info，存储域的基本元信息</li>
 *   <li>列：created_time(创建时间)、description(域描述)、owners(所有者列表)、readers(可读用户列表)等</li>
 * </ul>
 * 该表是YARN Timeline Service v2存储层的核心表之一，用于支持多租户场景下的时间线数据隔离与权限管理。
 */
public final class DomainTable extends BaseTable<DomainTable> {
}