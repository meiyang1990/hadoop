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

package org.apache.hadoop.mapred;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.hadoop.security.ssl.SSLFactory;

import static org.apache.hadoop.mapred.ShuffleHandler.TIMEOUT_HANDLER;
import static org.apache.hadoop.mapred.ShuffleHandler.LOG;

/**
 * MapReduce Shuffle阶段Netty HTTP服务的通道初始化器，负责配置新连接的ChannelPipeline
 * 为每个新接入的Map输出获取连接设置SSL、HTTP编解码、数据块处理和业务处理器
 */
public class ShuffleChannelInitializer extends ChannelInitializer<SocketChannel> {

  /** HTTP请求最大聚合长度 */
  public static final int MAX_CONTENT_LENGTH = 1 << 16;

  /** Shuffle处理器上下文，保存共享配置和处理器依赖 */
  private final ShuffleChannelHandlerContext handlerContext;
  /** SSL工厂，用于HTTPS加密连接，非加密时为null */
  private final SSLFactory sslFactory;


  /**
   * 构造Shuffle通道初始化器
   * @param ctx Shuffle处理器上下文，包含共享配置
   * @param sslFactory SSL工厂，用于HTTPS加密连接，不需要加密时传null
   */
  public ShuffleChannelInitializer(ShuffleChannelHandlerContext ctx, SSLFactory sslFactory) {
    this.handlerContext = ctx;
    this.sslFactory = sslFactory;
  }

  /**
   * 初始化新接入的Socket通道，按顺序组装ChannelPipeline处理器链
   * @param ch 新接入的Socket通道
   * @throws GeneralSecurityException SSL初始化异常
   * @throws IOException IO异常
   */
  @Override
  public void initChannel(SocketChannel ch) throws GeneralSecurityException, IOException {
    LOG.debug("ShuffleChannelInitializer init; channel='{}'", ch.id());

    ChannelPipeline pipeline = ch.pipeline();
    // 如果启用SSL，添加SSL处理器到管道最前端
    if (sslFactory != null) {
      pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
    }
    // 添加HTTP服务端编解码器，处理HTTP请求响应编解码
    pipeline.addLast("http", new HttpServerCodec());
    // 添加HTTP对象聚合器，将多个HTTP消息片段聚合为完整的FullHttpRequest/FullHttpResponse
    pipeline.addLast("aggregator", new HttpObjectAggregator(MAX_CONTENT_LENGTH));
    // 添加分块写处理器，支持大文件分块高效输出
    pipeline.addLast("chunking", new ChunkedWriteHandler());

    // An EventExecutorGroup could be specified to run in a
    // different thread than an I/O thread so that the I/O thread
    // is not blocked by a time-consuming task:
    // https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html
    // 添加Shuffle业务处理器，处理Map输出获取请求
    pipeline.addLast("shuffle", new ShuffleChannelHandler(handlerContext));

    // 添加连接超时处理器，回收空闲超时连接
    pipeline.addLast(TIMEOUT_HANDLER,
        new ShuffleHandler.TimeoutHandler(handlerContext.connectionKeepAliveTimeOut));
    // TODO factor security manager into pipeline
    // TODO factor out encode/decode to permit binary shuffle
    // TODO factor out decode of index to permit alt. models
  }
}