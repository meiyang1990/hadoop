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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * HDFS分层存储数据迁移工具包
 * <p>
 * Mover工具是HDFS分层存储体系下的数据块迁移工具，核心职责是：
 * 1. 扫描HDFS指定路径，检查数据块副本分布是否满足当前存储策略要求
 * 2. 对于不符合存储策略的数据块，将副本迁移到对应类型的存储介质上
 * 3. 保障分层存储中，冷热数据自动迁移到对应的存储层级，平衡存储成本与访问性能
 * </p>
 */
package org.apache.hadoop.hdfs.server.mover;