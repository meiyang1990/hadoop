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
package org.apache.hadoop.hdfs.server.datanode.web;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;

import org.slf4j.Logger;

import java.net.InetSocketAddress;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * 文件说明：DataNode节点内的简单HTTP反向代理处理器，用于将客户端HTTP请求转发给DataNode内部的目标HTTP服务，
 * 并将响应结果转发回客户端，适用于小体积、快速响应的代理场景。上层服务需要负责输入安全过滤。
 */
/**
 * 极简会话层HTTP代理处理器。负责将收到的HTTP请求代理转发到目标主机，并将目标主机返回的响应
 * 通过入站通道转发回原客户端。仅适用于响应较小、目标服务响应较快的场景。
 */
class SimpleHttpProxyHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private String uri;
  private Channel proxiedChannel;
  private final InetSocketAddress host;
  private final boolean isSecure;
  static final Logger LOG = DatanodeHttpServer.LOG;

  /**
   * 构造函数，初始化代理处理器，指定目标代理地址和是否为安全代理
   * @param host 目标代理服务地址
   * @param isSecure 是否为HTTPS安全代理
   */
  SimpleHttpProxyHandler(InetSocketAddress host, boolean isSecure) {
    this.host = host;
    this.isSecure = isSecure;
  }

  /**
   * 代理响应转发器，接收代理目标服务器返回的HTTP响应，转发给原始客户端通道
   */
  private static class Forwarder extends ChannelInboundHandlerAdapter {
    private final String uri;
    private final Channel client;

    private Forwarder(String uri, Channel client) {
      this.uri = uri;
      this.client = client;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      closeOnFlush(client);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
      client.writeAndFlush(msg).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
          if (future.isSuccess()) {
            // 转发成功，继续读取代理通道的下一段数据
            ctx.channel().read();
          } else {
            // 转发失败，关闭代理通道
            LOG.debug("Proxy failed. Cause: ", future.cause());
            future.channel().close();
          }
        }
      });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.debug("Proxy for " + uri + " failed. cause: ", cause);
      closeOnFlush(ctx.channel());
    }
  }

  /**
   * SSL重定向地址重写处理器，解决HTTP跳转问题：当外部客户端通过HTTPS访问代理，
   * 而后端HTTP服务返回HTTP开头的重定向地址时，将Location头中的http://改写为https://，
   * 避免客户端跳转失败（修复HDFS-17680问题）
   */
  private static final class SslRedirectRewriter extends ChannelInboundHandlerAdapter {
    private SslRedirectRewriter() { }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object message) {
      // 仅处理HTTP响应消息
      if (!(message instanceof HttpResponse)) {
        ctx.fireChannelRead(message);
        return;
      }

      HttpResponse response = (HttpResponse) message;
      String location = response.headers().get(HttpHeaderNames.LOCATION);
      // 如果存在Location头且为http开头，替换为https
      if (location != null && location.startsWith("http://")) {
        LOG.debug("Rewriting Location header from http to https: {}", location);
        location = location.replaceFirst("http://", "https://");
        response.headers().set(HttpHeaderNames.LOCATION, location);
      }
      ctx.fireChannelRead(response);
    }
  }

  @Override
  public void channelRead0
    (final ChannelHandlerContext ctx, final HttpRequest req) {
    uri = req.uri();
    final Channel client = ctx.channel();
    // 初始化Netty客户端Bootstrap，用于连接目标代理服务
    Bootstrap proxiedServer = new Bootstrap()
      .group(client.eventLoop())
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline p = ch.pipeline();
          // 添加HTTP请求编码器
          p.addLast(new HttpRequestEncoder());
          if (isSecure) {
            LOG.debug("Proxying secure request {} to {}", uri, host);
            // 添加HTTP响应解码器和SSL重定向地址重写器
            p.addLast(new HttpResponseDecoder(), new SslRedirectRewriter());
            // 为客户端通道添加HTTP响应编码器，用于编码转发给客户端的响应
            client.pipeline().addFirst(new HttpResponseEncoder());
          }
          // 添加响应转发处理器
          p.addLast(new Forwarder(uri, client));
        }
      });
    // 发起异步连接到目标代理服务
    ChannelFuture f = proxiedServer.connect(host);
    proxiedChannel = f.channel();
    // 添加连接完成监听器
    f.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          // 连接成功，移除客户端通道原有响应编码器（已在前面添加新的编码器）
          ctx.channel().pipeline().remove(HttpResponseEncoder.class);
          // 构造新的HTTP请求转发给目标服务
          HttpRequest newReq = new DefaultFullHttpRequest(HTTP_1_1, req.method(), req.uri());
          newReq.headers().add(req.headers());
          // 设置连接关闭头，请求完成后关闭连接
          newReq.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
          future.channel().writeAndFlush(newReq);
        } else {
          // 连接失败，返回500错误响应给客户端
          DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1,
            INTERNAL_SERVER_ERROR);
          resp.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
          LOG.info("Proxy " + uri + " failed. Cause: ", future.cause());
          ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
          client.close();
        }
      }
    });
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    // 客户端连接关闭时，关闭代理通道
    if (proxiedChannel != null) {
      proxiedChannel.close();
      proxiedChannel = null;
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Proxy for " + uri + " failed. cause: ", cause);
    }
    // 发生异常时，关闭代理通道和客户端通道
    if (proxiedChannel != null) {
      proxiedChannel.close();
      proxiedChannel = null;
    }
    ctx.close();
  }

  /**
   * 刷新缓冲区所有数据后关闭通道
   * @param ch 需要关闭的通道
   */
  private static void closeOnFlush(Channel ch) {
    if (ch.isActive()) {
      ch.writeAndFlush(Unpooled.EMPTY_BUFFER)
        .addListener(ChannelFutureListener.CLOSE);
    }
  }
}