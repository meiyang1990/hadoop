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
package org.apache.hadoop.hdfs.net;

import java.io.Closeable;
import org.apache.hadoop.classification.InterfaceAudience;
import java.io.IOException;
import java.net.SocketTimeoutException;

/**
 * HDFS网络连接服务端接口，定义了接收客户端Peer连接的通用能力
 * 为不同传输实现（如普通TCP、域套接字等）提供统一的服务端抽象
 */
@InterfaceAudience.Private
public interface PeerServer extends Closeable {
  /**
   * 设置服务端接收缓冲区大小
   * 
   * @param size 接收缓冲区大小（字节）
   * @throws IOException 如果设置失败抛出IO异常
   */
  public void setReceiveBufferSize(int size) throws IOException;

  /**
   * 获取当前服务端接收缓冲区大小
   *
   * @return 当前接收缓冲区大小（字节）
   * @throws IOException 如果获取失败抛出IO异常
   */
  int getReceiveBufferSize() throws IOException;

  /**
   * 阻塞等待并接收新的客户端连接，返回对应Peer对象
   * 方法会阻塞直到有新连接建立或超时
   *
   * @return 新建立连接对应的Peer对象
   * @exception IOException 等待连接过程中发生IO错误时抛出
   * @exception SecurityException 安全管理器不允许接受连接时抛出
   * @exception SocketTimeoutException 已设置超时且等待超时时抛出
   */
  public Peer accept() throws IOException, SocketTimeoutException;

  /**
   * 获取服务端监听地址的字符串表示，用于日志和调试
   *
   * @return 服务端监听地址的字符串描述
   */
  public String getListeningString();

  /**
   * 关闭服务端，释放关联资源（如监听Socket等）
   *
   * @throws IOException 关闭过程中发生IO错误时抛出
   */
  public void close() throws IOException;
}