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
 * MapReduce ApplicationMaster 指标监控包，提供 MapReduce 应用运行过程中的
 * 指标收集、统计与暴露能力，用于监控作业运行状态和性能，支撑集群运维与作业调优。
 * 所有类均为MapReduce应用内部私有实现，不对外暴露公共API。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.metrics;
import org.apache.hadoop.classification.InterfaceAudience;