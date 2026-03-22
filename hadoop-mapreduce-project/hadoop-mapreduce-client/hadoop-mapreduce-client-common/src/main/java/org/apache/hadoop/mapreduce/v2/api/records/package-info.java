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
 * MapReduce V2 API 数据记录包，定义了MapReduce客户端与服务端交互过程中
 * 使用的所有核心数据结构接口，包括作业、任务、计数器、任务尝试等核心实体
 * 这些数据结构用于在客户端、ResourceManager、ApplicationMaster之间传递状态信息
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.api.records;
import org.apache.hadoop.classification.InterfaceAudience;