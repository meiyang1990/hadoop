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
 * MapReduce ApplicationMaster 中作业与任务管理核心包
 * <p>
 * 该包定义了MapReduce作业运行过程中作业(Job)、任务(Task)、任务尝试(TaskAttempt)
 * 的核心抽象接口与基础实现，负责维护作业运行时的状态、事件流转与进度管理，
 * 是ApplicationMaster中作业执行逻辑的核心组成部分。
 * </p>
 * 包内所有API均为MapReduce内部私有API，不对外公开使用。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.job;
import org.apache.hadoop.classification.InterfaceAudience;