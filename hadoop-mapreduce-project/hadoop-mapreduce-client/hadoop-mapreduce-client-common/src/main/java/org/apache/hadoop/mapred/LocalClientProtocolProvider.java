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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;

/**
 * 本地模式客户端协议提供者
 * 当MapReduce运行在本地模式时，提供LocalJobRunner实例作为客户端协议实现，用于本地作业运行
 */
@InterfaceAudience.Private
public class LocalClientProtocolProvider extends ClientProtocolProvider {

  /**
   * 根据配置创建本地模式客户端协议实例
   * @param conf 配置对象
   * @return 如果是本地模式返回LocalJobRunner实例，否则返回null
   * @throws IOException 创建过程中抛出IO异常
   */
  @Override
  public ClientProtocol create(Configuration conf) throws IOException {
    // 获取当前配置的运行框架名称
    String framework =
        conf.get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    // 如果不是本地框架，返回null让其他提供者处理
    if (!MRConfig.LOCAL_FRAMEWORK_NAME.equals(framework)) {
      return null;
    }
    // 本地模式强制设置map数量为1
    conf.setInt(JobContext.NUM_MAPS, 1);

    // 创建并返回本地作业运行器实例
    return new LocalJobRunner(conf);
  }

  /**
   * 根据指定地址创建客户端协议实例（本地模式不支持网络地址连接）
   * @param addr 服务端地址
   * @param conf 配置对象
   * @return 始终返回null，因为本地模式不需要网络连接
   */
  @Override
  public ClientProtocol create(InetSocketAddress addr, Configuration conf) {
    return null; // LocalJobRunner doesn't use a socket
  }

  /**
   * 关闭客户端协议实例，本地模式不需要额外清理资源
   * @param clientProtocol 要关闭的客户端协议实例
   */
  @Override
  public void close(ClientProtocol clientProtocol) {
    // no clean up required
  }

}