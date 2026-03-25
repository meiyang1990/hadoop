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
 * MapReduce应用Master中作业和任务相关事件包，
 * 定义了作业运行过程中各类状态变更、任务更新等事件类型，
 * 用于实现应用内部基于事件驱动的状态流转与模块间通信。
 * 所有事件均为MapReduce应用内部私有，不对外公开。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.job.event;
import org.apache.hadoop.classification.InterfaceAudience;