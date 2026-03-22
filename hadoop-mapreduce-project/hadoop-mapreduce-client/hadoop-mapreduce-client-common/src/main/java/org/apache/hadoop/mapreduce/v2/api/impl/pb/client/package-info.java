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
 * MapReduce V2 API 基于Protobuf序列化的客户端实现包
 * <p>
 * 该包负责封装MapReduce客户端与服务端之间基于Protobuf的RPC通信逻辑，
 * 实现了MapReduce客户端API的底层PB协议编解码，为上层用户API提供透明的远程调用支持，
 * 仅作为MapReduce框架内部私有实现，不对外暴露公共API。
 * </p>
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.api.impl.pb.client;
import org.apache.hadoop.classification.InterfaceAudience;