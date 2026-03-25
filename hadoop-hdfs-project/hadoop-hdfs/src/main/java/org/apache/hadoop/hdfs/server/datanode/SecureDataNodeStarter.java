// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.SecurityUtil;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.net.BindException;

/**
 * 文件级注释：安全环境下DataNode启动器，通过commons-daemon框架实现，先以特权用户绑定特权端口，再将端口资源交给降权后的DataNode使用
 * 
 * Utility class to start a datanode in a secure cluster, first obtaining 
 * privileged resources before main startup and handing them to the datanode.
 */
public class SecureDataNodeStarter implements Daemon {
  /**
   * 存储安全环境下DataNode启动需要提前获取的特权资源（已绑定的端口套接字）
   */
  public static class SecureResources {
    private final boolean isSaslEnabled;
    private final boolean isRpcPortPrivileged;
    private final boolean isHttpPortPrivileged;

    private final ServerSocket streamingSocket;
    private final ServerSocketChannel httpServerSocket;

    public SecureResources(ServerSocket streamingSocket, ServerSocketChannel
        httpServerSocket, boolean saslEnabled, boolean rpcPortPrivileged,
        boolean httpPortPrivileged) {
      this.streamingSocket = streamingSocket;
      this.httpServerSocket = httpServerSocket;
      this.isSaslEnabled = saslEnabled;
      this.isRpcPortPrivileged = rpcPortPrivileged;
      this.isHttpPortPrivileged = httpPortPrivileged;
    }

    public ServerSocket getStreamingSocket() { return streamingSocket; }

    public ServerSocketChannel getHttpServerChannel() {
      return httpServerSocket;
    }

    public boolean isSaslEnabled() {
      return isSaslEnabled;
    }

    public boolean isRpcPortPrivileged() {
      return isRpcPortPrivileged;
    }

    public boolean isHttpPortPrivileged() {
      return isHttpPortPrivileged;
    }
  }
  
  private String [] args;
  private SecureResources resources;

  /**
   * 初始化方法：由commons-daemon框架调用，提前获取DataNode需要的特权资源
   * @param context 守护进程上下文，包含启动参数
   * @throws Exception 初始化失败抛出异常
   */
  @Override
  public void init(DaemonContext context) throws Exception {
    System.err.println("Initializing secure datanode resources");
    // 加载HDFS配置，确保读取hdfs-site.xml中的配置项
    Configuration conf = new HdfsConfiguration();
    
    // 保存命令行参数，后续传递给DataNode主启动逻辑
    args = context.getArguments();
    // 提前获取所有需要的特权端口资源
    resources = getSecureResources(conf);
  }

  /**
   * 启动方法：由commons-daemon框架调用，将提前获取的特权资源交给DataNode完成后续启动
   * @throws Exception 启动失败抛出异常
   */
  @Override
  public void start() throws Exception {
    System.err.println("Starting regular datanode initialization");
    DataNode.secureMain(args, resources);
  }

  @Override public void destroy() {}
  @Override public void stop() throws Exception { /* Nothing to do */ }

  /**
   * 提前为DataNode绑定并获取需要的特权端口资源，包括数据传输端口和HTTP信息端口
   * @param conf Hadoop配置对象
   * @return 包含已绑定套接字和特权标记的资源对象
   * @throws Exception 绑定失败抛出异常
   */
  @VisibleForTesting
  public static SecureResources getSecureResources(Configuration conf)
      throws Exception {
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    boolean isSaslEnabled =
        DataTransferSaslUtil.getSaslPropertiesResolver(conf) != null;
    boolean isRpcPrivileged;
    boolean isHttpPrivileged = false;

    System.err.println("isSaslEnabled:" + isSaslEnabled);
    // 获取DataNode数据传输服务绑定地址
    InetSocketAddress streamingAddr  = DataNode.getStreamingAddr(conf);
    int socketWriteTimeout = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        HdfsConstants.WRITE_TIMEOUT);
    int backlogLength = conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);

    // 根据是否需要超时选择不同的ServerSocket创建方式
    ServerSocket ss = (socketWriteTimeout > 0) ? 
        ServerSocketChannel.open().socket() : new ServerSocket();
    try {
      // 绑定数据传输端口
      ss.bind(streamingAddr, backlogLength);
    } catch (BindException e) {
      // 绑定失败后追加地址信息，抛出更清晰的异常
      BindException newBe = appendMessageToBindException(e,
          streamingAddr.toString());
      throw newBe;
    }

    // 校验是否绑定到了配置指定的端口
    if (ss.getLocalPort() != streamingAddr.getPort()) {
      throw new RuntimeException(
          "Unable to bind on specified streaming port in secure "
              + "context. Needed " + streamingAddr.getPort() + ", got "
              + ss.getLocalPort());
    }
    // 检查当前绑定端口是否为特权端口（<1024）
    isRpcPrivileged = SecurityUtil.isPrivilegedPort(ss.getLocalPort());
    System.err.println("Opened streaming server at " + streamingAddr);

    // 如果启用HTTP服务，提前绑定HTTP信息端口（仅HTTP需要特权端口，HTTPS不需要）
    final ServerSocketChannel httpChannel;
    if (policy.isHttpEnabled()) {
      httpChannel = ServerSocketChannel.open();
      InetSocketAddress infoSocAddr = DataNode.getInfoAddr(conf);
      try {
        // 绑定HTTP信息端口
        httpChannel.socket().bind(infoSocAddr);
      } catch (BindException e) {
        // 绑定失败追加地址信息，抛出清晰异常
        BindException newBe = appendMessageToBindException(e,
            infoSocAddr.toString());
        throw newBe;
      }
      InetSocketAddress localAddr = (InetSocketAddress) httpChannel.socket()
        .getLocalSocketAddress();

      // 校验是否绑定到配置指定的端口
      if (localAddr.getPort() != infoSocAddr.getPort()) {
        throw new RuntimeException("Unable to bind on specified info port in " +
            "secure context. Needed " + infoSocAddr.getPort() + ", got " +
             ss.getLocalPort());
      }
      System.err.println("Successfully obtained privileged resources (streaming port = "
          + ss + " ) (http listener port = " + localAddr.getPort() +")");

      // 检查HTTP端口是否为特权端口
      isHttpPrivileged = SecurityUtil.isPrivilegedPort(localAddr.getPort());
      System.err.println("Opened info server at " + infoSocAddr);
    } else {
      httpChannel = null;
    }

    return new SecureResources(ss, httpChannel, isSaslEnabled,
        isRpcPrivileged, isHttpPrivileged);
  }

  /**
   * 为BindException追加绑定地址信息，增强错误信息可读性
   * @param e 原始绑定异常
   * @param msg 要追加的地址信息
   * @return 包含新错误信息、保留原有调用栈和原因的新异常
   */
  private static BindException appendMessageToBindException(BindException e,
      String msg) {
    BindException newBe = new BindException(e.getMessage() + " " + msg);
    newBe.initCause(e.getCause());
    newBe.setStackTrace(e.getStackTrace());
    return newBe;
  }
}