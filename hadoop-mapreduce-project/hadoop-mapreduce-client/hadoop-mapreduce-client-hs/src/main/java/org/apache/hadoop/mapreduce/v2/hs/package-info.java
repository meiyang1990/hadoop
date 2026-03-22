// 这个文件已经全部加上中文注释
/*
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

/**
 * MapReduce 作业历史服务器核心实现包，提供已完成MapReduce作业的历史数据存储、查询与展示能力。
 * 该包包含历史服务器核心服务、作业历史索引管理、已完成作业状态恢复查询等核心功能，
 * 让用户可以在作业运行完成后查看作业运行日志、统计指标和执行详情，
 * 是MapReduce任务运维与问题排查的重要支撑模块。
 */
package org.apache.hadoop.mapreduce.v2.hs;