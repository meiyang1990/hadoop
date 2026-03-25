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
 * MapReduce V2 服务端客户端通信协议的请求/响应记录包。
 * 该包包含了所有MapReduce应用协议中客户端与服务端交互消息的数据结构定义，
 * 用于封装各类请求和响应参数，为Hadoop自有RPC框架提供序列化/反序列化的数据载体。
 */
package org.apache.hadoop.mapreduce.v2.api.protocolrecords;