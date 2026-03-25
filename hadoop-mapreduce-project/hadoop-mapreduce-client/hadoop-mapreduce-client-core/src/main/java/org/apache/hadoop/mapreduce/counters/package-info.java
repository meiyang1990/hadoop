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
 * @file package-info.java
 * @brief MapReduce计数器功能包，提供不同类型MapReduce作业计数器的核心实现
 *
 * 核心职责：
 * 定义并实现MapReduce作业运行过程中的各类统计计数器，用于对作业、任务的运行指标进行统计，
 * 支持用户自定义计数器，方便开发者对作业运行状态进行监控和分析，为作业调优提供数据支撑。
 * 设计背景请参考JIRA问题: MAPREDUCE-901。
 */
@InterfaceStability.Evolving
package org.apache.hadoop.mapreduce.counters;

import org.apache.hadoop.classification.InterfaceStability;