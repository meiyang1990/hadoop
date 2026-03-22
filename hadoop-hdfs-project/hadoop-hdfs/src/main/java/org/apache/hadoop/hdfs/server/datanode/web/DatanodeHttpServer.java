// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.hdfs.server.datanode.web;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.web.webhdfs.DataNodeUGIProvider;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.nio.channels.ServerSocketChannel;
import java.security.GeneralSecurityException;
import java.util.Enumeration;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_INTERNAL_PROXY_PORT;

/**
 * HDFS DataNode HTTP服务端，提供Web UI和WebHDFS访问能力，基于Jetty+Netty实现混合架构。
 * Jetty处理管理页面等Servlet请求，Netty处理WebHDFS数据传输请求以提升性能。
 */
public class DatanodeHttpServer implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(DatanodeHttpServer.class);
  // HttpServer threads are only used for the web UI and basic servlets, so
  // set them to the minimum possible
  private static final int HTTP_SELECTOR_THREADS = 2;
  private static final int HTTP_ACCEPTOR_THREADS = 1;
  // Jetty 9.4.x: Adding one more thread to HTTP_MAX_THREADS.
  private static final int HTTP_MAX_THREADS =
      HTTP_SELECTOR_THREADS + HTTP_ACCEPTOR_THREADS + 5;
  private final HttpServer2 infoServer;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ServerSocketChannel externalHttpChannel;
  private final ServerBootstrap httpServer;
  private final SSLFactory sslFactory;
  private final ServerBootstrap httpsServer;
  private final Configuration conf;
  private final Configuration confForCreate;
  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;

  /**
   * 构造DataNode HTTP服务端，初始化Jetty信息服务器和Netty HTTP/HTTPS服务端。
   * @param conf Hadoop配置对象
   * @param datanode 当前DataNode实例
   * @param externalHttpChannel 外部绑定的ServerSocketChannel（供JSVC等特权绑定场景使用，可为null）
   * @throws IOException 初始化失败时抛出异常
   */
  public DatanodeHttpServer(final Configuration conf,
        final DataNode datanode,
        final ServerSocketChannel externalHttpChannel)
        throws IOException {
    this.conf = conf;

    Configuration confForInfoServer = new Configuration(conf);
    // 限制Jetty线程数，管理页面不需要大量线程
    confForInfoServer.setInt(HttpServer2.HTTP_MAX_THREADS_KEY,
        HTTP_MAX_THREADS);
    confForInfoServer.setInt(HttpServer2.HTTP_SELECTOR_COUNT_KEY,
        HTTP_SELECTOR_THREADS);
    confForInfoServer.setInt(HttpServer2.HTTP_ACCEPTOR_COUNT_KEY,
        HTTP_ACCEPTOR_THREADS);
    int proxyPort =
        confForInfoServer.getInt(DFS_DATANODE_HTTP_INTERNAL_PROXY_PORT, 0);
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setName("datanode")
        .setConf(confForInfoServer)
        // 设置管理员访问控制列表
        .setACL(new AccessControlList(conf.get(DFS_ADMIN, " ")))
        // 获取Spnego认证需要的主机名
        .hostName(getHostnameForSpnegoPrincipal(confForInfoServer))
        // 绑定本地代理端口，Netty将管理请求转发给Jetty处理
        .addEndpoint(URI.create("http://localhost:" + proxyPort))
        .setFindPort(true);

    final boolean xFrameEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    final String xFrameOptionValue = conf.getTrimmed(
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    // 配置X-Frame-Options防点击劫持
    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

    this.infoServer = builder.build();

    // 向Jetty上下文注入共享对象
    this.infoServer.setAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE, conf);
    this.infoServer.setAttribute("datanode", datanode);
    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    // 注册块扫描报告Servlet
    this.infoServer.addServlet(null, "/blockScannerReport",
        BlockScanner.Servlet.class);
    DataNodeUGIProvider.init(conf);
    this.infoServer.start();
    // 获取Jetty实际绑定地址，供Netty转发请求使用
    final InetSocketAddress jettyAddr = infoServer.getConnectorAddress(0);

    this.confForCreate = new Configuration(conf);
    // 文件创建时使用000 umask，权限由HDFS本身控制
    confForCreate.set(FsPermission.UMASK_LABEL, "000");

    // 初始化Netty EventLoop线程组
    this.bossGroup = new NioEventLoopGroup();
    this.workerGroup = new NioEventLoopGroup();
    this.externalHttpChannel = externalHttpChannel;
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    // 加载配置指定的Netty过滤器处理器
    final ChannelHandler[] handlers = getFilterHandlers(conf);

    if (policy.isHttpEnabled()) {
      this.httpServer = new ServerBootstrap().group(bossGroup, workerGroup)
            .childHandler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                // 添加HTTP编解码器
                p.addLast(new HttpRequestDecoder(),
                    new HttpResponseEncoder());
                // 添加用户自定义过滤器处理器
                if (handlers != null) {
                  for (ChannelHandler c : handlers) {
                    p.addLast(c);
                  }
                }
                // 添加分块写入支持和URL请求分发器
                p.addLast(
                    new ChunkedWriteHandler(),
                    new URLDispatcher(jettyAddr, conf, confForCreate, false));
              }
            });

      // 配置Netty写缓冲区水位线，控制背压
      this.httpServer.childOption(
          ChannelOption.WRITE_BUFFER_WATER_MARK,
          new WriteBufferWaterMark(conf.getInt(
               DFSConfigKeys.DFS_WEBHDFS_NETTY_LOW_WATERMARK,
               DFSConfigKeys.DFS_WEBHDFS_NETTY_LOW_WATERMARK_DEFAULT),
               conf.getInt(
                   DFSConfigKeys.DFS_WEBHDFS_NETTY_HIGH_WATERMARK,
                   DFSConfigKeys.DFS_WEBHDFS_NETTY_HIGH_WATERMARK_DEFAULT)));

      // 配置Channel工厂，支持外部已绑定的Channel（JSVC场景）
      if (externalHttpChannel == null) {
        httpServer.channel(NioServerSocketChannel.class);
      } else {
        httpServer.channelFactory(new ChannelFactory<NioServerSocketChannel>() {
          @Override
          public NioServerSocketChannel newChannel() {
            return new NioServerSocketChannel(externalHttpChannel) {
              // 通道已经由外部JSVC绑定过，此处bind方法空实现
              @Override
              protected void doBind(SocketAddress localAddress)
                  throws Exception {
              }
            };
          }
        });
      }
    } else {
      this.httpServer = null;
    }

    // 如果HTTPS启用，初始化HTTPS服务端
    if (policy.isHttpsEnabled()) {
      this.sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
      try {
        sslFactory.init();
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
      this.httpsServer = new ServerBootstrap().group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline p = ch.pipeline();
              // 添加SSL处理器
              p.addLast(
                  new SslHandler(sslFactory.createSSLEngine()),
                  new HttpRequestDecoder(),
                  new HttpResponseEncoder());
              // 添加用户自定义过滤器处理器
              if (handlers != null) {
                for (ChannelHandler c : handlers) {
                  p.addLast(c);
                }
              }
              // 添加分块写入支持和URL请求分发器，标记为HTTPS请求
              p.addLast(
                  new ChunkedWriteHandler(),
                  new URLDispatcher(jettyAddr, conf, confForCreate, true));
            }
          });
    } else {
      this.httpsServer = null;
      this.sslFactory = null;
    }
  }

  /**
   * 获取Spnego认证主体对应的主机名，用于Kerberos认证。
   * @param conf Hadoop配置对象
   * @return 主机名字符串
   */
  private static String getHostnameForSpnegoPrincipal(Configuration conf) {
    String addr = conf.getTrimmed(DFS_DATANODE_HTTP_ADDRESS_KEY, null);
    if (addr == null) {
      addr = conf.getTrimmed(DFS_DATANODE_HTTPS_ADDRESS_KEY,
          DFS_DATANODE_HTTPS_ADDRESS_DEFAULT);
    }
    InetSocketAddress inetSocker = NetUtils.createSocketAddr(addr);
    return inetSocker.getHostString();
  }

  /**
   * 从配置中加载初始化自定义Netty Channel过滤器处理器。
   * @param configuration Hadoop配置对象
   * @return 初始化完成的处理器数组
   */
  private ChannelHandler[] getFilterHandlers(Configuration configuration) {
    if (configuration == null) {
      return null;
    }
    // If the hdfs-site.xml has the proper configs for filter classes, use them.
    Class<?>[] classes =
        configuration.getClasses(
            DFSConfigKeys.DFS_DATANODE_HTTPSERVER_FILTER_HANDLERS);

    // else use the hard coded class from the default configuration.
    if (classes == null) {
      classes =
          configuration.getClasses(
              DFSConfigKeys.DFS_DATANODE_HTTPSERVER_FILTER_HANDLERS_DEFAULT);
    }

    // if we are not able to find any handlers, let us fail since running
    // with Csrf will is a security hole. Let us abort the startup.
    if(classes == null)  {
      return null;
    }

    ChannelHandler[] handlers = new ChannelHandler[classes.length];
    for (int i = 0; i < classes.length; i++) {
      LOG.debug("Loading filter handler {}", classes[i].getName());
      try {
        // 通过反射调用静态初始化方法获取初始化参数，再调用构造函数创建实例
        Method initializeState = classes[i].getDeclaredMethod("initializeState",
            Configuration.class);
        Constructor<?> constructor =
            classes[i].getDeclaredConstructor(initializeState.getReturnType());
        handlers[i] = (ChannelHandler) constructor.newInstance(
            initializeState.invoke(null, configuration));
      } catch (NoSuchMethodException | InvocationTargetException
          | IllegalAccessException | InstantiationException
          | IllegalArgumentException e) {
        LOG.error("Failed to initialize handler {}", classes[i].toString());
        throw new RuntimeException(e);
      }
    }
    return (handlers);
  }

  /**
   * 获取HTTP服务绑定的地址。
   * @return HTTP地址
   */
  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  /**
   * 获取HTTPS服务绑定的地址。
   * @return HTTPS地址
   */
  public InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  /**
   * 启动HTTP和HTTPS服务，完成端口绑定。
   * @throws IOException 端口绑定失败时抛出异常
   */
  public void start() throws IOException {
    if (httpServer != null) {
      InetSocketAddress infoAddr = DataNode.getInfoAddr(conf);
      // 绑定端口并获取实际绑定地址
      httpAddress = getChannelLocalAddress(httpServer, infoAddr);
      // 更新配置，保存实际绑定地址
      conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY,
          NetUtils.getHostPortString(httpAddress));
      LOG.info("Listening for HTTP traffic on {}", httpAddress);
    }

    if (httpsServer != null) {
      InetSocketAddress secInfoSocAddr =
          NetUtils.createSocketAddr(conf.getTrimmed(
              DFS_DATANODE_HTTPS_ADDRESS_KEY,
              DFS_DATANODE_HTTPS_ADDRESS_DEFAULT));
      // 绑定端口并获取实际绑定地址
      httpsAddress = getChannelLocalAddress(httpsServer, secInfoSocAddr);
      // 更新配置，保存实际绑定地址
      conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY,
          NetUtils.getHostPortString(httpsAddress));
      LOG.info("Listening for HTTPS traffic on {}", httpsAddress);
    }
  }

  /**
   * 绑定服务端到指定地址，返回实际绑定的本地地址。
   * @param server Netty ServerBootstrap实例
   * @param address 要绑定的地址
   * @return 实际绑定的地址
   * @throws IOException 绑定失败时抛出异常
   */
  private InetSocketAddress getChannelLocalAddress(
      ServerBootstrap server, InetSocketAddress address) throws IOException {
    ChannelFuture f = server.bind(address);
    try {
      f.syncUninterruptibly();
    } catch (Throwable e) {
      if (e instanceof BindException) {
        throw NetUtils.wrapException(null, 0, address.getHostName(),
                address.getPort(), (SocketException) e);
      } else {
        throw e;
      }
    }
    return (InetSocketAddress) f.channel().localAddress();
  }

  @Override
  /**
   * 关闭HTTP服务，释放所有资源。
   * @throws IOException 关闭失败时抛出IO异常
   */
  public void close() throws IOException {
    // 关闭Netty线程组
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    // 销毁SSL工厂资源
    if (sslFactory != null) {
      sslFactory.destroy();
    }
    // 关闭外部绑定的Channel
    if (externalHttpChannel != null) {
      externalHttpChannel.close();
    }
    // 停止Jetty信息服务器
    try {
      infoServer.stop();
    } catch (Exception e