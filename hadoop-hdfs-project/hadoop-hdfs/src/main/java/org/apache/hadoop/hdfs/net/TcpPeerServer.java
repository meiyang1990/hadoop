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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.channels.ServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.ipc.Server;

/**
 * TCP协议Peer服务端实现，负责监听并接受客户端的TCP连接，
 * 为HDFS数据传输提供基于TCP的连接接入能力，支持普通和安全两种模式。
 */
@InterfaceAudience.Private
public class TcpPeerServer implements PeerServer {
  static final Logger LOG = LoggerFactory.getLogger(TcpPeerServer.class);

  private final ServerSocket serverSocket;

  /**
   * 构造非安全模式的TcpPeerServer，绑定指定地址并开始监听连接请求。
   *
   * @param socketWriteTimeout  socket写入超时时间，单位毫秒。大于0时开启NIO模式
   * @param bindAddr  需要绑定监听的本地地址
   * @param backlogLength  TCP连接请求积压队列的最大长度
   * @throws IOException  绑定或创建ServerSocket失败时抛出
   */
  public TcpPeerServer(int socketWriteTimeout,
                       InetSocketAddress bindAddr,
                       int backlogLength) throws IOException {
    this.serverSocket = (socketWriteTimeout > 0) ?
          ServerSocketChannel.open().socket() : new ServerSocket();
    Server.bind(serverSocket, bindAddr, backlogLength);
  }

  /**
   * 构造安全模式的TcpPeerServer，使用安全启动提供的已初始化ServerSocket。
   *
   * @param secureResources  安全DataNode启动提供的安全资源对象，包含已配置好的监听Socket
   */
  public TcpPeerServer(SecureResources secureResources) {
    this.serverSocket = secureResources.getStreamingSocket();
  }
  
  /**
   * 获取当前服务正在监听流式数据连接的地址。
   *
   * @return  监听地址，包含本地IP和端口
   */
  public InetSocketAddress getStreamingAddr() {
    return new InetSocketAddress(
        serverSocket.getInetAddress().getHostAddress(),
        serverSocket.getLocalPort());
  }

  @Override
  public void setReceiveBufferSize(int size) throws IOException {
    this.serverSocket.setReceiveBufferSize(size);
  }

  @Override
  public int getReceiveBufferSize() throws IOException {
    return this.serverSocket.getReceiveBufferSize();
  }

  @Override
  /**
   * 接受一个新的 incoming TCP连接，并封装为Peer对象返回。
   *
   * @return  封装了新连接的Peer对象
   * @throws IOException  接受连接失败时抛出
   * @throws SocketTimeoutException  接受连接超时抛出
   */
  public Peer accept() throws IOException, SocketTimeoutException {
    Peer peer = DFSUtilClient.peerFromSocket(serverSocket.accept());
    return peer;
  }

  @Override
  public String getListeningString() {
    return serverSocket.getLocalSocketAddress().toString();
  }
  
  @Override
  /**
   * 关闭当前服务端监听Socket，释放相关资源。
   * 关闭失败会记录错误日志，不抛出异常。
   */
  public void close() throws IOException {
    try {
      serverSocket.close();
    } catch(IOException e) {
      LOG.error("error closing TcpPeerServer: ", e);
    }
  }

  @Override
  public String toString() {
    return "TcpPeerServer(" + getListeningString() + ")";
  }
}