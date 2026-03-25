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
 * MapReduce ApplicationMaster 作业运行时核心实现包
 * <p>
 * 本包包含 MapReduce 作业在 YARN ApplicationMaster 侧的核心实现逻辑，
 * 包括作业（Job）、任务（Task）、尝试（TaskAttempt）等核心运行时实体的具体实现，
 * 负责作业执行过程中的状态管理、进度跟踪、容错处理等核心功能，是 MapReduce 应用
 * 运行时的核心实现层，仅对内公开，不对外提供公共API。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.job.impl;
import org.apache.hadoop.classification.InterfaceAudience;