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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HostRestrictingAuthorizationFilter;
import org.apache.hadoop.hdfs.server.common.HostRestrictingAuthorizationFilter.HttpInteraction;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Datanode Web服务Netty管道的IP地址访问限制授权处理器。
 * 集成HostRestrictingAuthorizationFilter实现IP访问控制，当请求被禁止时返回403响应，
 * 请求合法时则转发给后续处理器继续处理。
 */
@InterfaceAudience.Private
@Sharable
final class HostRestrictingAuthorizationFilterHandler
    extends SimpleChannelInboundHandler<HttpRequest> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HostRestrictingAuthorizationFilterHandler.class);
  /** 共享的IP访问限制过滤器实例，初始化后无状态，可被多通道复用 */
  private final
  HostRestrictingAuthorizationFilter hostRestrictingAuthorizationFilter;

  /**
   * 构造函数，使用已初始化完成的IP访问限制过滤器。
   * 过滤器初始化后为无状态，可在多个Netty管道间共享，避免重复初始化开销。
   * @param hostRestrictingAuthorizationFilter 已完成初始化的IP访问限制过滤器
   */
  public HostRestrictingAuthorizationFilterHandler(
      HostRestrictingAuthorizationFilter hostRestrictingAuthorizationFilter) {
    this.hostRestrictingAuthorizationFilter =
        hostRestrictingAuthorizationFilter;
  }

  /**
   * 无参构造函数，内部自行完成过滤器的初始化，从Hadoop配置中读取访问规则。
   */
  public HostRestrictingAuthorizationFilterHandler() {
    Configuration conf = new Configuration();
    this.hostRestrictingAuthorizationFilter = initializeState(conf);
  }

  /**
   * 为Datanode HTTP服务初始化IP访问限制过滤器，适配Servlet Filter接口要求。
   * 从Hadoop配置中读取IP访问规则，完成过滤器初始化。
   * @param conf Hadoop配置对象
   * @return 初始化完成的IP访问限制过滤器
   * @throws IllegalStateException 过滤器初始化失败时抛出
   */
  public static HostRestrictingAuthorizationFilter
  initializeState(Configuration conf) {
    String confName = HostRestrictingAuthorizationFilter.HDFS_CONFIG_PREFIX +
        HostRestrictingAuthorizationFilter.RESTRICTION_CONFIG;
    String confValue = conf.get(confName);
    // 未配置则传入空字符串
    confValue = (confValue == null ? "" : confValue);

    Map<String, String> confMap =
        ImmutableMap.of(HostRestrictingAuthorizationFilter.RESTRICTION_CONFIG
            , confValue);
    FilterConfig fc =
        new DatanodeHttpServer.MapBasedFilterConfig(
            HostRestrictingAuthorizationFilter.class.getName(), confMap);
    HostRestrictingAuthorizationFilter hostRestrictingAuthorizationFilter =
        new HostRestrictingAuthorizationFilter();
    try {
      hostRestrictingAuthorizationFilter.init(fc);
    } catch (ServletException e) {
      throw new IllegalStateException(
          "Failed to initialize HostRestrictingAuthorizationFilter.", e);
    }
    return hostRestrictingAuthorizationFilter;
  }

  /**
   * 发送HTTP响应并关闭连接，添加Connection: close响应头。
   * @param ctx Netty通道处理上下文
   * @param resp 要发送的HTTP响应
   */
  private static void sendResponseAndClose(ChannelHandlerContext ctx,
      DefaultHttpResponse resp) {
    resp.headers().set(CONNECTION, CLOSE);
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx,
      final HttpRequest req) throws Exception {
    // 将Netty请求包装后交给IP访问过滤器处理
    hostRestrictingAuthorizationFilter
        .handleInteraction(new NettyHttpInteraction(ctx, req));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception in " + this.getClass().getSimpleName(), cause);
    // 发生异常返回500错误并关闭连接
    sendResponseAndClose(ctx,
        new DefaultHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR));
  }

  /**
   * 适配Netty请求的HttpInteraction实现，将Netty HTTP请求适配给通用IP访问过滤器。
   */
  private static final class NettyHttpInteraction implements HttpInteraction {

    private final ChannelHandlerContext ctx;
    private final HttpRequest req;
    private boolean committed;

    /**
     * 构造函数，封装Netty请求和上下文。
     * @param ctx Netty通道处理上下文
     * @param req Netty HTTP请求对象
     */
    public NettyHttpInteraction(ChannelHandlerContext ctx, HttpRequest req) {
      this.committed = false;
      this.ctx = ctx;
      this.req = req;
    }

    @Override
    public boolean isCommitted() {
      return committed;
    }

    @Override
    public String getRemoteAddr() {
      // 获取客户端IP地址
      return ((InetSocketAddress) ctx.channel().remoteAddress()).
          getAddress().getHostAddress();
    }

    @Override
    public String getQueryString() {
      try {
        // 解析请求查询参数部分
        return (new URI(req.uri()).getQuery());
      } catch (URISyntaxException e) {
        return null;
      }
    }

    @Override
    public String getRequestURI() {
      String uri = req.uri();
      // Netty的uri包含查询参数，需要截取出不含查询参数的请求路径
      return (uri.substring(0, uri.indexOf("?") >= 0 ? uri.indexOf("?") :
          uri.length()));
    }

    @Override
    public String getRemoteUser() {
      // 从URL查询参数中解析用户名参数
      QueryStringDecoder queryString = new QueryStringDecoder(req.getUri());
      List<String> p = queryString.parameters().get(UserParam.NAME);
      String user = (p == null ? null : p.get(0));
      return (new UserParam(user).getValue());
    }

    @Override
    public String getMethod() {
      return req.getMethod().name();
    }

    @Override
    public void proceed() {
      // 请求通过授权，增加引用计数后转发给下一个处理器
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    }

    @Override
    public void sendError(int code, String message) {
      // 请求被拒绝，返回错误码并关闭连接
      HttpResponseStatus status = new HttpResponseStatus(code, message);
      sendResponseAndClose(ctx, new DefaultHttpResponse(HTTP_1_1, status));
      this.committed = true;
    }
  }
}