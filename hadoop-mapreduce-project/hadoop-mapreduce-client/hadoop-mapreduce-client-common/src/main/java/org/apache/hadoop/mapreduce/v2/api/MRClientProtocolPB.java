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
package org.apache.hadoop.mapreduce.v2.api;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.yarn.proto.MRClientProtocol.MRClientProtocolService;

/**
 * MR客户端协议PB序列化接口，基于Protobuf实现MapReduce客户端到ApplicationMaster的阻塞式RPC通信协议
 * 定义了Hadoop RPC层需要的协议信息，继承Protobuf生成的阻塞服务接口
 */
@ProtocolInfo(
    protocolName = "org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB",
    protocolVersion = 1)
public interface MRClientProtocolPB extends MRClientProtocolService.BlockingInterface {
  
}