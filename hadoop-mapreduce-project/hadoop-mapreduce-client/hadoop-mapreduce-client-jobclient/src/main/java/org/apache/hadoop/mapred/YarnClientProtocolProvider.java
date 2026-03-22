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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;

/**
 * YARN模式下的MapReduce客户端协议提供者，用于创建连接YARN集群的客户端协议实例
 * 遵循Hadoop插件式架构设计，仅当集群运行在YARN框架下生效
 */
public class YarnClientProtocolProvider extends ClientProtocolProvider {

  /**
   * 根据配置创建YARN模式的客户端协议实例
   * @param conf Hadoop配置对象
   * @return 若配置为YARN框架则返回YARNRunner实例，否则返回null
   * @throws IOException 创建失败时抛出IO异常
   */
  @Override
  public ClientProtocol create(Configuration conf) throws IOException {
    // 检查当前框架是否配置为YARN，匹配才创建对应实例
    if (MRConfig.YARN_FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME))) {
      return new YARNRunner(conf);
    }
    return null;
  }

  /**
   * 根据指定地址创建YARN模式的客户端协议实例
   * @param addr YARN ResourceManager地址
   * @param conf Hadoop配置对象
   * @return YARNRunner客户端协议实例
   * @throws IOException 创建失败时抛出IO异常
   */
  @Override
  public ClientProtocol create(InetSocketAddress addr, Configuration conf)
      throws IOException {
    return create(conf);
  }

  /**
   * 关闭客户端协议实例，释放相关资源
   * @param clientProtocol 要关闭的客户端协议实例
   * @throws IOException 关闭失败时抛出IO异常
   */
  @Override
  public void close(ClientProtocol clientProtocol) throws IOException {
    // 仅对本提供者创建的YARNRunner实例执行关闭操作
    if (clientProtocol instanceof YARNRunner) {
      ((YARNRunner)clientProtocol).close();
    }
  }
}