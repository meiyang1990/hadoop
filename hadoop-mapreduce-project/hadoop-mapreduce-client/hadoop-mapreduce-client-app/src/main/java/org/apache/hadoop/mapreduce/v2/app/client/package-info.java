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
 * MapReduce ApplicationMaster 客户端交互模块
 * 提供客户端与运行中的 MapReduce 应用 Master 进行通信的核心接口与实现，
 * 支持作业状态查询、作业控制（杀死作业等）等客户端操作，
 * 供 YARN 外部客户端和 ResourceManager 与 ApplicationMaster 交互使用。
 * 该包仅供 Hadoop 内部使用，不对外公开API。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.app.client;
import org.apache.hadoop.classification.InterfaceAudience;