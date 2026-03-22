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
 * MapReduce Reduce阶段核心实现包，提供Reduce任务执行的底层核心逻辑。
 * 核心职责包括：从Map节点拉取Map输出数据、合并排序Map输出、调用用户Reduce方法处理数据，
 * 是MapReduce计算框架中Reduce阶段的核心实现模块。
 * 本包仅对内提供能力，属于MapReduce客户端核心私有包，不对外公开稳定API。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.mapreduce.task.reduce;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;