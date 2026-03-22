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
import java.net.SocketTimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.net.unix.DomainSocket;

/**
 * 基于Unix域套接字实现的Peer服务端，用于同一主机上进程间的高速通信
 * 实现PeerServer接口，负责接收HDFS客户端的域套接字连接并创建Peer实例
 */
@InterfaceAudience.Private
public class DomainPeerServer implements PeerServer {
  static final Logger LOG = LoggerFactory.getLogger(DomainPeerServer.class);
  // 底层监听的Unix域套接字对象
  private final DomainSocket sock;

  /**
   * 构造方法，使用已绑定监听的域套接字创建DomainPeerServer
   * @param sock 已完成bind和listen的域套接字实例
   */
  DomainPeerServer(DomainSocket sock) {
    this.sock = sock;
  }

  /**
   * 构造方法，根据指定路径和端口创建并绑定监听域套接字
   * @param path 域套接字文件路径
   * @param port 端口号，用于生成唯一路径
   * @throws IOException 创建绑定域套接字失败时抛出
   */
  public DomainPeerServer(String path, int port) 
      throws IOException {
    this(DomainSocket.bindAndListen(DomainSocket.getEffectivePath(path, port)));
  }
  
  /**
   * 获取当前域套接字绑定的文件路径
   * @return 绑定的文件路径字符串
   */
  public String getBindPath() {
    return sock.getPath();
  }

  @Override
  public void setReceiveBufferSize(int size) throws IOException {
    // 设置域套接字接收缓冲区大小
    sock.setAttribute(DomainSocket.RECEIVE_BUFFER_SIZE, size);
  }

  @Override
  public int getReceiveBufferSize() throws IOException {
    // 获取域套接字接收缓冲区大小
    return sock.getAttribute(DomainSocket.RECEIVE_BUFFER_SIZE);
  }

  @Override
  public Peer accept() throws IOException, SocketTimeoutException {
    // 接受新的客户端连接，获取通信套接字
    DomainSocket connSock = sock.accept();
    Peer peer = null;
    boolean success = false;
    try {
      // 基于连接套接字创建DomainPeer实例
      peer = new DomainPeer(connSock);
      success = true;
      return peer;
    } finally {
      // 构造失败时清理资源，避免句柄泄漏
      if (!success) {
        if (peer != null) peer.close();
        connSock.close();
      }
    }
  }

  @Override
  public String getListeningString() {
    // 返回监听地址描述字符串，用于日志和调试
    return "unix:" + sock.getPath();
  }
  
  @Override
  public void close() throws IOException {
    try {
      // 关闭底层监听的域套接字
      sock.close();
    } catch (IOException e) {
      LOG.error("error closing DomainPeerServer: ", e);
    }
  }

  @Override
  public String toString() {
    // 返回当前服务端的字符串描述
    return "DomainPeerServer(" + getListeningString() + ")";
  }
}