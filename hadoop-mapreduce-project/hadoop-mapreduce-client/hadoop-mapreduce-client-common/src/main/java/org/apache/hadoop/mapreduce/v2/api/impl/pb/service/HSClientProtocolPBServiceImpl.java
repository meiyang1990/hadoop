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
 * @file HSClientProtocolPBServiceImpl.java
 * 历史服务器客户端协议Protobuf服务实现类，为Hadoop MapReduce历史服务器客户端提供PB序列化RPC服务实现
 * 属于mapreduce客户端公共模块，负责将PB格式的RPC请求转发给实际的本地协议实现
 */
package org.apache.hadoop.mapreduce.v2.api.impl.pb.service;

import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocolPB;

/**
 * 历史服务器客户端协议PB服务实现类
 * 继承通用MR客户端协议PB实现，实现历史服务器专属PB协议接口，
 * 在YARN RPC框架中负责将Protobuf序列化的客户端请求转发给实际业务实现层
 */
public class HSClientProtocolPBServiceImpl extends MRClientProtocolPBServiceImpl 
  implements HSClientProtocolPB {

  /**
   * 构造历史服务器客户端协议PB服务实现
   * @param impl 实际业务逻辑的协议实现实例，处理转发过来的RPC请求
   */
  public HSClientProtocolPBServiceImpl(HSClientProtocol impl) {
    super(impl);
  } 
}