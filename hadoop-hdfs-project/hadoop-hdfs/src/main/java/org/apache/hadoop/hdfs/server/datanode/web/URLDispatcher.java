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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler;

import java.net.InetSocketAddress;

import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX;

/**
 * DataNode Web服务HTTP请求分发器，根据请求URL路径将请求分发到不同处理器处理。
 * 负责将WebHDFS请求路由到WebHdfsHandler，其他请求代理转发到指定后端服务。
 * 作为Netty通道处理器，处理完成后会替换自身为对应业务处理器，提升后续处理效率。
 */
class URLDispatcher extends SimpleChannelInboundHandler<HttpRequest> {
  // 代理转发目标地址
  private final InetSocketAddress proxyHost;
  // Hadoop配置对象
  private final Configuration conf;
  // 用于创建文件操作的配置对象
  private final Configuration confForCreate;
  // 是否启用安全传输
  private final boolean isSecure;

  /**
   * 构造URL分发器，初始化分发依赖配置
   * @param proxyHost 非WebHDFS请求的代理转发目标地址
   * @param conf 基础Hadoop配置
   * @param confForCreate 创建文件操作专用配置
   * @param isSecure 是否启用安全传输
   */
  URLDispatcher(InetSocketAddress proxyHost, Configuration conf,
                Configuration confForCreate, boolean isSecure) {
    this.proxyHost = proxyHost;
    this.conf = conf;
    this.confForCreate = confForCreate;
    this.isSecure = isSecure;
  }

  /**
   * 读取HTTP请求，根据URL路径分发请求到对应处理器
   * 替换当前分发器为对应业务处理器，后续请求直接由业务处理器处理
   * @param ctx Netty通道上下文
   * @param req HTTP请求对象
   * @throws Exception 处理过程中可能抛出的异常
   */
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req)
      throws Exception {
    // 获取请求URI
    String uri = req.uri();
    // 获取当前通道的处理器流水线
    ChannelPipeline p = ctx.pipeline();
    // 判断是否为WebHDFS请求
    if (uri.startsWith(WEBHDFS_PREFIX)) {
      // 创建WebHDFS处理器
      WebHdfsHandler h = new WebHdfsHandler(conf, confForCreate);
      // 将当前分发器替换为WebHDFS处理器
      p.replace(this, WebHdfsHandler.class.getSimpleName(), h);
      // 触发WebHDFS处理器处理请求
      h.channelRead0(ctx, req);
    } else {
      // 创建HTTP代理处理器
      SimpleHttpProxyHandler h = new SimpleHttpProxyHandler(proxyHost, isSecure);
      // 将当前分发器替换为代理处理器
      p.replace(this, SimpleHttpProxyHandler.class.getSimpleName(), h);
      // 触发代理处理器转发请求
      h.channelRead0(ctx, req);
    }
  }
}