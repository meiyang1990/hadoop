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

package org.apache.hadoop.mapreduce.protocol;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * 客户端协议提供者抽象基类，负责创建和关闭MapReduce客户端与服务端通信的ClientProtocol实例
 * 不同部署方式（本地运行、YARN集群等）可提供不同的实现，解耦协议创建与客户端调用
 */
@InterfaceAudience.Private
public abstract class ClientProtocolProvider {
  
  /**
   * 根据配置创建ClientProtocol客户端协议实例，自动解析地址配置
   * @param conf Hadoop配置对象
   * @return 初始化完成的ClientProtocol实例
   * @throws IOException 创建过程中IO或配置错误时抛出
   */
  public abstract ClientProtocol create(Configuration conf) throws IOException;
  
  /**
   * 根据指定地址和配置创建ClientProtocol客户端协议实例
   * @param addr 服务端地址
   * @param conf Hadoop配置对象
   * @return 初始化完成的ClientProtocol实例
   * @throws IOException 创建过程中IO或连接错误时抛出
   */
  public abstract ClientProtocol create(InetSocketAddress addr,
      Configuration conf) throws IOException;

  /**
   * 关闭指定的ClientProtocol实例，释放相关资源
   * @param clientProtocol 需要关闭的客户端协议实例
   * @throws IOException 关闭过程中IO错误时抛出
   */
  public abstract void close(ClientProtocol clientProtocol) throws IOException;

}