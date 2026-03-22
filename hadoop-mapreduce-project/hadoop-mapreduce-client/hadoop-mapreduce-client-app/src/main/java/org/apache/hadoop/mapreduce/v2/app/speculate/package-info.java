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
 * MapReduce Application推测执行功能包。
 * 核心职责：实现MapReduce任务的推测执行机制，当检测到某个任务执行明显慢于同作业其他任务时，
 * 会启动一个备份任务同时执行，由最先完成的任务提供结果，从而规避慢节点导致的作业整体执行延迟。
 * 提供多种推测判定策略和数据统计能力，支持配置化调整推测执行行为。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.speculate;
import org.apache.hadoop.classification.InterfaceAudience;