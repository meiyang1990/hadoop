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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import org.apache.hadoop.classification.VisibleForTesting;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 文件级注释：HDFS离线镜像查看Web服务端，加载fsimage文件后对外提供只读的WebHDFS API，
 * 用于在不启动HDFS集群的情况下查看命名空间内容
 * 
 * WebImageViewer加载fsimage镜像文件，对外提供只读的WebHDFS API访问命名空间。
 */
public class WebImageViewer implements Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(WebImageViewer.class);

  private Channel channel;
  private InetSocketAddress address;

  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ChannelGroup allChannels;
  private final Configuration conf;

  /**
   * 构造WebImageViewer实例，使用默认配置
   * @param address 服务监听地址
   */
  public WebImageViewer(InetSocketAddress address) {
    this(address, new Configuration());
  }

  /**
   * 构造WebImageViewer实例，使用指定配置
   * @param address 服务监听地址
   * @param conf Hadoop配置对象
   */
  public WebImageViewer(InetSocketAddress address, Configuration conf) {
    this.address = address;
    this.bossGroup = new NioEventLoopGroup();
    this.workerGroup = new NioEventLoopGroup();
    this.allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    this.bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NioServerSocketChannel.class);
    this.conf = conf;
    UserGroupInformation.setConfiguration(conf);
  }

  /**
   * 启动WebImageViewer服务并阻塞等待中断，加载指定fsimage对外提供服务
   * @param fsimage fsimage文件路径
   * @throws IOException 加载fsimage失败时抛出
   * @throws RuntimeException 配置开启安全认证时抛出
   */
  public void start(String fsimage) throws IOException {
    try {
      // 不支持安全认证模式，要求使用simple认证
      if (UserGroupInformation.isSecurityEnabled()) {
        throw new RuntimeException(
            "WebImageViewer does not support secure mode. To start in " +
                "non-secure mode, pass -D" +
                CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION +
                "=simple");
      }
      // 初始化Netty服务端并加载fsimage
      initServer(fsimage);
      // 阻塞等待服务关闭
      channel.closeFuture().await();
    } catch (InterruptedException e) {
      LOG.info("Interrupted. Stopping the WebImageViewer.");
      close();
    }
  }

  /**
   * 初始化Web服务端，加载fsimage并启动HTTP服务
   * @param fsimage fsimage文件路径
   * @throws IOException 加载fsimage失败时抛出
   * @throws InterruptedException 服务绑定端口被中断时抛出
   */
  @VisibleForTesting
  public void initServer(String fsimage)
          throws IOException, InterruptedException {
    // 加载并解析fsimage为内存命名空间
    final FSImageLoader loader = FSImageLoader.load(fsimage);

    // 设置HTTP请求处理通道初始化器
    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        // 添加HTTP编解码器和业务处理器
        p.addLast(new HttpRequestDecoder(),
          new StringEncoder(),
          new HttpResponseEncoder(),
          new FSImageHandler(loader, allChannels));
      }
    });

    // 绑定端口启动服务
    channel = bootstrap.bind(address).sync().channel();
    allChannels.add(channel);

    // 更新实际监听地址（如果指定端口为0会自动分配）
    address = (InetSocketAddress) channel.localAddress();
    LOG.info("WebImageViewer started. Listening on " + address.toString() + ". Press Ctrl+C to stop the viewer.");
  }

  /**
   * 获取服务实际监听端口
   * @return WebImageViewer监听的端口号
   */
  @VisibleForTesting
  public int getPort() {
    return address.getPort();
  }

  @Override
  public void close() {
    // 关闭所有连接并优雅关闭Netty线程池
    allChannels.close().awaitUninterruptibly();
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }
}