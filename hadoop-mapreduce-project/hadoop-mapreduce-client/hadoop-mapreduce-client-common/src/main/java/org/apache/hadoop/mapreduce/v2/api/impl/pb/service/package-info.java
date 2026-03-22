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
 * MapReduce V2 API Protobuf 序列化服务实现包。
 * 本包提供基于 Protobuf 协议的 MapReduce 客户端与服务端之间
 * RPC 通信的服务端 stub 实现，负责处理序列化后的请求消息，
 * 转发给实际业务逻辑处理，并将返回结果序列化回响应。
 * 所有实现均为 MapReduce 框架私有，不对外公开。
 */
@InterfaceAudience.Private
package org.apache.hadoop.mapreduce.v2.api.impl.pb.service;
import org.apache.hadoop.classification.InterfaceAudience;