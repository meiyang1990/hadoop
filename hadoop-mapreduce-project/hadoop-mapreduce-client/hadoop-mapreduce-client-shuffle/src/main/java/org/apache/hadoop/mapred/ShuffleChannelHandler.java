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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleHeader;
import org.eclipse.jetty.http.HttpHeader;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.mapred.ShuffleHandler.AUDITLOG;
import static org.apache.hadoop.mapred.ShuffleHandler.CONNECTION_CLOSE;
import static org.apache.hadoop.mapred.ShuffleHandler.FETCH_RETRY_DELAY;
import static org.apache.hadoop.mapred.ShuffleHandler.IGNORABLE_ERROR_MESSAGE;
import static org.apache.hadoop.mapred.ShuffleHandler.RETRY_AFTER_HEADER;
import static org.apache.hadoop.mapred.ShuffleHandler.TIMEOUT_HANDLER;
import static org.apache.hadoop.mapred.ShuffleHandler.TOO_MANY_REQ_STATUS;
import static org.apache.hadoop.mapred.ShuffleHandler.LOG;

/**
 * MapReduce Shuffle阶段基于Netty的HTTP处理器，处理Reduce端获取Map端输出数据的请求。
 * 负责验证请求合法性、读取Map输出文件并通过HTTP流式返回给Reduce端，每个Map输出前携带ShuffleHeader头信息。
 */
public class ShuffleChannelHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private final ShuffleChannelHandlerContext handlerCtx;

  /**
   * 构造Shuffle通道处理器，持有上下文环境
   * @param ctx Shuffle处理器上下文，包含连接数限制、缓存、密钥等公共配置
   */
  ShuffleChannelHandler(ShuffleChannelHandlerContext ctx) {
    handlerCtx = ctx;
  }

  /**
   * 拆分逗号分隔的MapID列表为单个MapID
   * @param mapq 原始请求中的MapID参数列表
   * @return 拆分后的单个MapID列表
   */
  private List<String> splitMaps(List<String> mapq) {
    if (null == mapq) {
      return null;
    }
    final List<String> ret = new ArrayList<>();
    for (String s : mapq) {
      Collections.addAll(ret, s.split(","));
    }
    return ret;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx)
      throws Exception {
    LOG.debug("Executing channelActive; channel='{}'", ctx.channel().id());
    // 连接数计数器加1
    int numConnections = handlerCtx.activeConnections.incrementAndGet();
    // 如果超过最大允许连接数，拒绝请求并提示客户端重试
    if ((handlerCtx.maxShuffleConnections > 0) &&
        (numConnections > handlerCtx.maxShuffleConnections)) {
      LOG.info(String.format("Current number of shuffle connections (%d) is " +
              "greater than the max allowed shuffle connections (%d)",
          handlerCtx.allChannels.size(), handlerCtx.maxShuffleConnections));

      Map<String, String> headers = new HashMap<>(1);
      // 添加重试延迟头，客户端收到后会优雅退避，不会视为获取失败
      headers.put(RETRY_AFTER_HEADER, String.valueOf(FETCH_RETRY_DELAY));
      sendError(ctx, "", TOO_MANY_REQ_STATUS, headers);
    } else {
      // 连接数未超限，接受连接并添加到活动连接列表
      super.channelActive(ctx);
      handlerCtx.allChannels.add(ctx.channel());
      LOG.debug("Added channel: {}, channel id: {}. Accepted number of connections={}",
          ctx.channel(), ctx.channel().id(), handlerCtx.activeConnections.get());
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("Executing channelInactive; channel='{}'", ctx.channel().id());
    super.channelInactive(ctx);
    // 连接关闭，连接数计数器减1
    int noOfConnections = handlerCtx.activeConnections.decrementAndGet();
    LOG.debug("New value of Accepted number of connections={}", noOfConnections);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
    Channel channel = ctx.channel();
    LOG.debug("Received HTTP request: {}, channel='{}'", request, channel.id());

    // 只允许GET方法请求
    if (request.method() != GET) {
      sendError(ctx, METHOD_NOT_ALLOWED);
      return;
    }
    // 检查Shuffle版本兼容性
    String shuffleVersion = ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION;
    String httpHeaderName = ShuffleHeader.DEFAULT_HTTP_HEADER_NAME;
    if (request.headers() != null) {
      shuffleVersion = request.headers().get(ShuffleHeader.HTTP_HEADER_VERSION);
      httpHeaderName = request.headers().get(ShuffleHeader.HTTP_HEADER_NAME);
      LOG.debug("Received from request header: ShuffleVersion={} header name={}, channel id: {}",
          shuffleVersion, httpHeaderName, channel.id());
    }
    // 版本不兼容直接返回错误
    if (request.headers() == null ||
        !ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(httpHeaderName) ||
        !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(shuffleVersion)) {
      sendError(ctx, "Incompatible shuffle request version", BAD_REQUEST);
      return;
    }
    // 解析请求URL参数
    final Map<String, List<String>> q =
        new QueryStringDecoder(request.uri()).parameters();

    // 解析keepAlive参数
    final List<String> keepAliveList = q.get("keepAlive");
    boolean keepAliveParam = false;
    if (keepAliveList != null && keepAliveList.size() == 1) {
      keepAliveParam = Boolean.parseBoolean(keepAliveList.get(0));
      if (LOG.isDebugEnabled()) {
        LOG.debug("KeepAliveParam: {} : {}, channel id: {}",
            keepAliveList, keepAliveParam, channel.id());
      }
    }
    // 获取并拆分MapID列表、ReduceID和JobID参数
    final List<String> mapIds = splitMaps(q.get("map"));
    final List<String> reduceQ = q.get("reduce");
    final List<String> jobQ = q.get("job");
    if (LOG.isDebugEnabled()) {
      LOG.debug("RECV: " + request.uri() +
          "\n  mapId: " + mapIds +
          "\n  reduceId: " + reduceQ +
          "\n  jobId: " + jobQ +
          "\n  keepAlive: " + keepAliveParam +
          "\n  channel id: " + channel.id());
    }

    // 检查必填参数是否存在
    if (mapIds == null || reduceQ == null || jobQ == null) {
      sendError(ctx, "Required param job, map and reduce", BAD_REQUEST);
      return;
    }
    // 检查参数数量是否合法，只能有一个JobID和一个ReduceID
    if (reduceQ.size() != 1 || jobQ.size() != 1) {
      sendError(ctx, "Too many job/reduce parameters", BAD_REQUEST);
      return;
    }

    int reduceId;
    String jobId;
    try {
      // 解析ReduceID为整数
      reduceId = Integer.parseInt(reduceQ.get(0));
      jobId = jobQ.get(0);
    } catch (NumberFormatException e) {
      sendError(ctx, "Bad reduce parameter", BAD_REQUEST);
      return;
    } catch (IllegalArgumentException e) {
      sendError(ctx, "Bad job parameter", BAD_REQUEST);
      return;
    }
    final String reqUri = request.uri();
    if (null == reqUri) {
      sendError(ctx, FORBIDDEN);
      return;
    }
    // 构造成功响应对象
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
    try {
      // 验证请求签名合法性，防止非法访问
      verifyRequest(jobId, ctx, request, response,
          new URL("http", "", handlerCtx.port, reqUri));
    } catch (IOException e) {
      LOG.warn("Shuffle failure ", e);
      sendError(ctx, e.getMessage(), UNAUTHORIZED);
      return;
    }

    // 预缓存Map输出元信息，提前计算总长度
    Map<String, MapOutputInfo> mapOutputInfoMap = new HashMap<>();
    ChannelPipeline pipeline = channel.pipeline();
    ShuffleHandler.TimeoutHandler timeoutHandler =
        (ShuffleHandler.TimeoutHandler)pipeline.get(TIMEOUT_HANDLER);
    // 传输过程中禁用超时，传输完成后重新开启
    timeoutHandler.setEnabledTimeout(false);
    // 获取Job对应用户名，用于权限校验
    String user = handlerCtx.userRsrc.get(jobId);

    try {
      // 填充响应头，预验证所有文件可访问并计算总内容长度
      populateHeaders(mapIds, jobId, user, reduceId,
          response, keepAliveParam, mapOutputInfoMap);
    } catch(IOException e) {
      LOG.error("Shuffle error while populating headers. Channel id: " + channel.id(), e);
      sendError(ctx, getErrorMessage(e), INTERNAL_SERVER_ERROR);
      return;
    }

    // 写入响应头
    channel.write(response);

    // 为当前请求构造Reduce上下文，保存请求相关信息
    boolean keepAlive = keepAliveParam || handlerCtx.connectionKeepAliveEnabled;
    ReduceContext reduceContext = new ReduceContext(mapIds, reduceId, ctx,
        user, mapOutputInfoMap, jobId, keepAlive);

    // 开始发送Map输出数据
    sendMap(reduceContext);
  }

  /**
   * 按批次发送Map输出数据，控制并发打开文件数避免文件句柄耗尽，每发送完一个自动触发下一个发送。
   * 该方法先被channelRead0调用发送第一批，后续每个发送完成后由监听器触发下一个发送。
   * @param reduceContext 当前Reduce请求上下文，包含待发送Map列表、进度信息等
   */
  public void sendMap(ReduceContext reduceContext) {
    LOG.trace("Executing sendMap; channel='{}'", reduceContext.ctx.channel().id());
    // 还有未发送的Map输出，继续发送下一个
    if (reduceContext.getMapsToSend().get() <
        reduceContext.getMapIds().size()) {
      int nextIndex = reduceContext.getMapsToSend().getAndIncrement();
      String mapId = reduceContext.getMapIds().get(nextIndex);

      try {
        MapOutputInfo info = reduceContext.getInfoMap().get(mapId);
        // 如果没预缓存，实时获取元信息
        if (info == null) {
          info = getMapOutputInfo(mapId, reduceContext.getReduceId(),
              reduceContext.getJobId(), reduceContext.getUser());
        }
        LOG.trace("Calling sendMapOutput; channel='{}'", reduceContext.ctx.channel().id());
        // 发送单个Map输出
        ChannelFuture nextMap = sendMapOutput(
            reduceContext.getCtx().channel(),
            reduceContext.getUser(), mapId,
            reduceContext.getReduceId(), info);
        // 添加完成监听器，发送完成后触发下一个Map发送
        nextMap.addListener(new ReduceMapFileCount(this, reduceContext));
      } catch (IOException e) {
        LOG.error("Shuffle error: {}; channel={}", e, reduceContext.ctx.channel().id());
        // 响应头已经发送，只能关闭连接处理错误
        reduceContext.ctx.channel().close();
      }
    }
  }

  /**
   * 递归拼接所有异常层级的错误信息
   * @param t 原始异常对象
   * @return 拼接后的完整错误信息
   */
  private String getErrorMessage(Throwable t) {
    StringBuilder sb = new StringBuilder(t.getMessage());
    while (t.getCause() != null) {
      sb.append(t.getCause().getMessage());
      t = t.getCause();
    }
    return sb.toString();
  }

  /**
   * 获取指定Map输出的元信息，从缓存读取或从索引文件解析
   * @param mapId Map任务ID
   * @param reduce Reduce任务ID
   * @param jobId Job ID
   * @param user 作业提交用户名
   * @return Map输出元信息，包含数据文件路径和索引信息
   * @throws IOException 获取元信息失败时抛出异常
   */
  protected MapOutputInfo getMapOutputInfo(String mapId, int reduce, String jobId, String user)
      throws IOException {
    ShuffleHandler.AttemptPathInfo pathInfo;
    try {
      ShuffleHandler.AttemptPathIdentifier identifier = new ShuffleHandler.AttemptPathIdentifier(
          jobId, user, mapId);
      // 从路径缓存获取Map输出文件路径
      pathInfo = handlerCtx.pathCache.get(identifier);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Retrieved pathInfo for " + identifier +
            " check for corresponding loaded messages to determine whether" +
            " it was loaded or cached");
      }
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new RuntimeException(e.getCause());
      }
    }

    // 从索引缓存获取当前Reduce分区的索引信息
    IndexRecord info =
        handlerCtx.indexCache.getIndexInformation(mapId, reduce, pathInfo.indexPath, user);

    if (LOG.isDebugEnabled()) {
      LOG.debug("getMapOutputInfo: jobId=" + jobId + ", mapId=" + mapId +
          ",dataFile=" + pathInfo.dataPath + ", indexFile=" +
          pathInfo.indexPath);
      LOG.debug("getMapOutputInfo: startOffset={}, partLength={} rawLength={}",
          info.startOffset, info.partLength, info.rawLength);
    }

    return new MapOutputInfo(pathInfo.dataPath, info);
  }

  /**
   * 预填充HTTP响应头，验证所有文件可访问并计算总内容长度
   * @param mapIds 待获取的MapID列表
   * @param jobId Job ID
   * @param user 作业提交用户名
   * @param reduce Reduce任务ID
   * @param response HTTP响应对象
   * @param keepAliveParam 是否请求长连接
   * @param mapOutputInfoMap 用于缓存Map输出元信息
   * @throws IOException 验证或读取失败时抛出异常
   */
  protected void populateHeaders(List<String> mapIds, String jobId,
                                 String user, int reduce, HttpResponse response,
                                 boolean keepAliveParam,
                                 Map<String, MapOutputInfo> mapOutputInfoMap)
      throws IOException {

    long contentLength =