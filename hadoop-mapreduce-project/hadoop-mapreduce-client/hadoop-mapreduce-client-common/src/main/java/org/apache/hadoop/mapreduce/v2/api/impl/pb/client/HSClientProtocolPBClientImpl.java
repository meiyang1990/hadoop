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

package org.apache.hadoop.mapreduce.v2.api.impl.pb.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocolPB;

/**
 * 历史服务器客户端协议Protobuf实现类
 * 负责实现与MapReduce历史服务器服务端的RPC通信，基于Protobuf序列化框架
 * 继承通用MR客户端PB实现，扩展提供历史服务器专属协议代理
 */
public class HSClientProtocolPBClientImpl extends MRClientProtocolPBClientImpl
  implements HSClientProtocol {

  /**
   * 构造历史服务器客户端协议PB代理，初始化RPC连接
   * @param clientVersion 客户端协议版本号
   * @param addr 历史服务器服务端地址
   * @param conf Hadoop配置对象
   * @throws IOException 初始化RPC代理失败时抛出
   */
  public HSClientProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    super();
    // 设置使用ProtobufRpcEngine2作为协议引擎
    RPC.setProtocolEngine(conf, HSClientProtocolPB.class,
        ProtobufRpcEngine2.class);
    // 获取历史服务器协议RPC代理对象
    proxy = (HSClientProtocolPB)RPC.getProxy(
        HSClientProtocolPB.class, clientVersion, addr, conf);
  }
}