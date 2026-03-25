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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.util.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.APPLICATION_JSON_UTF8;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX_LENGTH;

/**
 * 为离线fsimage文件提供只读WebHDFS API实现，支持通过HTTP接口查询fsimage中的文件系统元数据
 */
class FSImageHandler extends SimpleChannelInboundHandler<HttpRequest> {
  public static final Logger LOG =
      LoggerFactory.getLogger(FSImageHandler.class);
  // fsimage加载器实例，负责实际查询fsimage元数据
  private final FSImageLoader image;
  // 维护所有活跃连接的通道组
  private final ChannelGroup activeChannels;

  /**
   * 通道激活时，将当前连接加入活跃连接组
   */
  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    activeChannels.add(ctx.channel());
  }

  /**
   * 构造FSImageHandler，绑定fsimage加载器和活跃连接组
   * @param image fsimage加载器实例
   * @param activeChannels 活跃连接组
   * @throws IOException 初始化异常
   */
  FSImageHandler(FSImageLoader image, ChannelGroup activeChannels) throws IOException {
    this.image = image;
    this.activeChannels = activeChannels;
  }

  /**
   * 处理HTTP请求，解析请求参数并转发对应操作，返回JSON格式响应
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest request)
      throws Exception {
    // 仅支持GET方法，非GET请求返回错误
    if (request.method() != HttpMethod.GET) {
      DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1,
          METHOD_NOT_ALLOWED);
      resp.headers().set(CONNECTION, CLOSE);
      ctx.write(resp).addListener(ChannelFutureListener.CLOSE);
      return;
    }

    // 解析请求URL参数
    QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
    // 检查并提取请求路径，路径不合法则抛出异常
    String path = getPath(decoder);
    // 提取操作参数op
    final String op = getOp(decoder);
    // op参数必须存在，否则抛出参数异常
    if (op == null) {
      throw new IllegalArgumentException("Param op must be specified.");
    }

    final String content;
    // 根据op分发不同查询操作
    switch (op) {
    case "GETFILESTATUS":
      // 查询指定路径文件状态信息
      content = image.getFileStatus(path);
      break;
    case "LISTSTATUS":
      // 列出指定目录下所有文件状态
      content = image.listStatus(path);
      break;
    case "GETACLSTATUS":
      // 查询指定路径ACL权限信息
      content = image.getAclStatus(path);
      break;
    case "GETXATTRS":
      // 获取指定路径扩展属性
      List<String> names = getXattrNames(decoder);
      String encoder = getEncoder(decoder);
      content = image.getXAttrs(path, names, encoder);
      break;
    case "LISTXATTRS":
      // 列出指定路径所有扩展属性名称
      content = image.listXAttrs(path);
      break;
    case "GETCONTENTSUMMARY":
      // 获取指定路径存储空间使用汇总信息
      content = image.getContentSummary(path);
      break;
    default:
      // op参数不合法，抛出参数异常
      throw new IllegalArgumentException("Invalid value for webhdfs parameter"
          + " \"op\"");
    }

    LOG.info("op=" + op + " target=" + path);

    // 构造成功JSON响应并返回客户端
    DefaultFullHttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1,
        HttpResponseStatus.OK, Unpooled.wrappedBuffer(content
            .getBytes(StandardCharsets.UTF_8)));
    resp.headers().set(CONTENT_TYPE, APPLICATION_JSON_UTF8);
    resp.headers().set(CONTENT_LENGTH, resp.content().readableBytes());
    resp.headers().set(CONNECTION, CLOSE);
    ctx.write(resp).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  /**
   * 处理请求过程中抛出的异常，根据异常类型返回对应HTTP错误响应
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
          throws Exception {
    // 将非异常包装为异常，统一处理
    Exception e = cause instanceof Exception ? (Exception) cause : new
        Exception(cause);
    // 将异常序列化为JSON格式
    final String output = JsonUtil.toJsonString(e);
    ByteBuf content = Unpooled.wrappedBuffer(output.getBytes(StandardCharsets.UTF_8));
    // 默认返回500错误
    final DefaultFullHttpResponse resp = new DefaultFullHttpResponse(
            HTTP_1_1, INTERNAL_SERVER_ERROR, content);

    resp.headers().set(CONTENT_TYPE, APPLICATION_JSON_UTF8);
    // 根据异常类型调整HTTP响应状态码
    if (e instanceof IllegalArgumentException) {
      resp.setStatus(BAD_REQUEST);
    } else if (e instanceof FileNotFoundException) {
      resp.setStatus(NOT_FOUND);
    } else if (e instanceof IOException) {
      resp.setStatus(FORBIDDEN);
    }
    resp.headers().set(CONTENT_LENGTH, resp.content().readableBytes());
    resp.headers().set(CONNECTION, CLOSE);
    ctx.write(resp).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * 从请求参数中获取op参数，转为大写格式
   * @param decoder URL查询参数解码器
   * @return op参数值，不存在则返回null
   */
  private static String getOp(QueryStringDecoder decoder) {
    Map<String, List<String>> parameters = decoder.parameters();
    return parameters.containsKey("op")
        ? StringUtils.toUpperCase(parameters.get("op").get(0)) : null;
  }

  /**
   * 从请求参数中获取扩展属性名称列表
   * @param decoder URL查询参数解码器
   * @return 扩展属性名称列表
   */
  private static List<String> getXattrNames(QueryStringDecoder decoder) {
    Map<String, List<String>> parameters = decoder.parameters();
    return parameters.get("xattr.name");
  }

  /**
   * 从请求参数中获取编码格式参数
   * @param decoder URL查询参数解码器
   * @return 编码格式参数，不存在则返回null
   */
  private static String getEncoder(QueryStringDecoder decoder) {
    Map<String, List<String>> parameters = decoder.parameters();
    return parameters.containsKey("encoding") ? parameters.get("encoding").get(
        0) : null;
  }

  /**
   * 从请求路径中提取HDFS文件路径，验证前缀是否合法
   * @param decoder URL查询参数解码器
   * @return HDFS文件路径
   * @throws FileNotFoundException 路径前缀不合法时抛出
   */
  private static String getPath(QueryStringDecoder decoder)
          throws FileNotFoundException {
    String path = decoder.path();
    if (path.startsWith(WEBHDFS_PREFIX)) {
      return path.substring(WEBHDFS_PREFIX_LENGTH);
    } else {
      throw new FileNotFoundException("Path: " + path + " should " +
              "start with " + WEBHDFS_PREFIX);
    }
  }
}