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
 * MapReduce协议记录的Protobuf实现包，包含所有协议请求和响应记录的基于Protobuf的序列化实现。
 * 该包下所有实现均为MapReduce框架内部私有实现，不对外暴露公开API，负责将高层协议记录转换为Protobuf字节流，
 * 支撑MapReduce客户端与服务端之间的RPC通信序列化与反序列化。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;
import org.apache.hadoop.classification.InterfaceAudience;