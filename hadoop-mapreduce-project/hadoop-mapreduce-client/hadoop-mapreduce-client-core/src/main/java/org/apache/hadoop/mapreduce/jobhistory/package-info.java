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
 * MapReduce作业历史记录模块，提供作业运行完成后历史数据的存储、解析与查询能力。
 * 该模块负责记录MapReduce作业、任务、尝试级别的运行事件、指标信息等运行日志，
 * 支持作业历史服务对历史作业进行可视化展示和统计分析，是MapReduce作业运维和问题排查的核心支撑模块。
 * 本包仅为MapReduce内部使用，API稳定性不对外承诺。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.mapreduce.jobhistory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;