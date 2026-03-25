// 这个文件已经全部加上中文注释
/**
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
 * HDFS RPC协议基于Protocol Buffers的序列化实现包。
 * 负责将HDFS客户端与服务端之间的RPC调用请求和响应，转换为Protobuf格式进行编解码，
 * 实现跨语言、高效的分布式通信，支撑HDFS客户端与NameNode、DataNode之间的RPC交互。
 */
package org.apache.hadoop.hdfs.protocolPB;