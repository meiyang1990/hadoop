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

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_KEY;

import java.util.Map;

import javax.servlet.ServletException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter.HttpInteraction;
import org.slf4j.Logger;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

/**
 * DataNode Web服务的REST跨站请求伪造防护处理器，适配Netty管道与通用Hadoop CSRF过滤器
 * 将通用Hadoop CSRF过滤器集成到DataNode的Netty HTTP服务中，对入站请求进行CSRF校验
 * 校验通过则转发给下一级处理器，不通过则直接返回错误响应
 */
@InterfaceAudience.Private
@Sharable
final class RestCsrfPreventionFilterHandler
    extends SimpleChannelInboundHandler<HttpRequest> {

  private static final Logger LOG = DatanodeHttpServer.LOG;

  private final RestCsrfPreventionFilter restCsrfPreventionFilter;

  /**
   * 构造CSRF防护处理器，复用预先初始化好的CSRF过滤器实例
   * CSRF过滤器初始化后无状态，可在多个通道/管道间共享，避免重复初始化开销
   * @param restCsrfPreventionFilter 已完成初始化的CSRF防护过滤器实例
   */
  RestCsrfPreventionFilterHandler(
      RestCsrfPreventionFilter restCsrfPreventionFilter) {
    if(restCsrfPreventionFilter == null) {
      LOG.warn("Got null for restCsrfPreventionFilter - will not do any filtering.");
    }
    this.restCsrfPreventionFilter = restCsrfPreventionFilter;
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx,
      final HttpRequest req) throws Exception {
    // 过滤器存在则执行CSRF校验
    if(restCsrfPreventionFilter != null) {
      restCsrfPreventionFilter.handleHttpInteraction(new NettyHttpInteraction(
          ctx, req));
    } else {
      // 无有效过滤器直接放行请求
      new NettyHttpInteraction(ctx, req).proceed();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception in " + this.getClass().getSimpleName(), cause);
    // 返回500错误并关闭连接
    sendResponseAndClose(ctx,
        new DefaultHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR));
  }

  /**
   * 发送HTTP响应并关闭连接，设置Connection: close头
   * @param ctx Netty通道上下文
   * @param resp 要发送的HTTP响应
   */
  private static void sendResponseAndClose(ChannelHandlerContext ctx,
      DefaultHttpResponse resp) {
    // 设置连接关闭头
    resp.headers().set(CONNECTION, CLOSE);
    // 刷新响应并添加关闭连接监听器
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * 适配Netty HTTP请求的HttpInteraction实现，供CSRF过滤器调用
   * 封装Netty请求头、方法获取和后续处理逻辑，对接通用CSRF过滤器接口
   */
  private static final class NettyHttpInteraction implements HttpInteraction {

    private final ChannelHandlerContext ctx;
    private final HttpRequest req;

    /**
     * 构造Netty环境的HTTP交互实例
     * @param ctx Netty通道上下文
     * @param req 待处理的Netty HTTP请求
     */
    NettyHttpInteraction(ChannelHandlerContext ctx, HttpRequest req) {
      this.ctx = ctx;
      this.req = req;
    }

    @Override
    public String getHeader(String header) {
      return req.headers().get(header);
    }

    @Override
    public String getMethod() {
      return req.getMethod().name();
    }

    @Override
    public void proceed() {
      // 增加引用计数，将请求转发给下一级处理器处理
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    }

    @Override
    public void sendError(int code, String message) {
      // 构造错误响应并关闭连接
      HttpResponseStatus status = new HttpResponseStatus(code, message);
      sendResponseAndClose(ctx, new DefaultHttpResponse(HTTP_1_1, status));
    }
  }

  /**
   * 根据Hadoop配置初始化DataNode Web服务的CSRF防护过滤器
   * 读取配置参数并构造满足Servlet规范的过滤器配置，完成过滤器初始化
   * @param conf Hadoop配置对象
   * @return 初始化完成的CSRF过滤器实例，若CSRF防护未启用则返回null
   */
  public static RestCsrfPreventionFilter initializeState(
      Configuration conf) {
    // 判断配置是否启用CSRF防护
    if (!conf.getBoolean(DFS_WEBHDFS_REST_CSRF_ENABLED_KEY,
        DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT)) {
      return null;
    }
    // 获取过滤器类名和配置参数
    String restCsrfClassName = RestCsrfPreventionFilter.class.getName();
    Map<String, String> restCsrfParams = RestCsrfPreventionFilter
        .getFilterParams(conf, "dfs.webhdfs.rest-csrf.");
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    try {
      // 使用基于Map的过滤器配置完成初始化
      filter.init(new DatanodeHttpServer
          .MapBasedFilterConfig(restCsrfClassName, restCsrfParams));
    } catch (ServletException e) {
      throw new IllegalStateException(
          "Failed to initialize RestCsrfPreventionFilter.", e);
    }
    return(filter);
  }
}