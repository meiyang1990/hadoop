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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import org.apache.hadoop.util.Preconditions;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.stream.ChunkedStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.AclPermissionParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.LimitInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_MAX_AGE;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.LOCATION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HDFS_URI_SCHEME;
import static org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier.HDFS_DELEGATION_KIND;

/**
 * DataNode端WebHDFS REST API请求处理器，处理各类WebHDFS读写操作请求。
 * 继承Netty SimpleChannelInboundHandler，基于Netty实现HTTP异步请求处理，
 * 负责路由不同操作类型到对应处理方法，完成用户身份认证和请求日志记录。
 */
public class WebHdfsHandler extends SimpleChannelInboundHandler<HttpRequest> {
  static final Logger LOG = LoggerFactory.getLogger(WebHdfsHandler.class);
  static final Logger REQLOG = LoggerFactory.getLogger("datanode.webhdfs");
  public static final String WEBHDFS_PREFIX = WebHdfsFileSystem.PATH_PREFIX;
  public static final int WEBHDFS_PREFIX_LENGTH = WEBHDFS_PREFIX.length();
  public static final String APPLICATION_OCTET_STREAM =
    "application/octet-stream";
  public static final String APPLICATION_JSON_UTF8 =
      "application/json; charset=utf-8";

  public static final EnumSet<CreateFlag> EMPTY_CREATE_FLAG =
      EnumSet.noneOf(CreateFlag.class);

  private final Configuration conf;
  private final Configuration confForCreate;

  private String path;
  private ParameterParser params;
  private UserGroupInformation ugi;
  private DefaultHttpResponse resp = null;

  /**
   * 构造WebHDFS处理器，初始化配置并设置用户名和ACL权限正则匹配规则。
   * @param conf 通用Hadoop配置
   * @param confForCreate 创建文件操作使用的配置
   * @throws IOException 初始化失败时抛出异常
   */
  public WebHdfsHandler(Configuration conf, Configuration confForCreate)
    throws IOException {
    this.conf = conf;
    this.confForCreate = confForCreate;
    /** set user pattern based on configuration file */
    UserParam.setUserPattern(
        conf.get(HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
            HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));
    AclPermissionParam.setAclPermissionPattern(
        conf.get(HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY,
            HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT));
  }

  @Override
  /**
   * 读取HTTP请求，解析参数并在对应用户身份下执行处理逻辑，最后记录访问日志。
   */
  public void channelRead0(final ChannelHandlerContext ctx,
                           final HttpRequest req) throws Exception {
    // 校验请求路径以WebHDFS前缀开头
    Preconditions.checkArgument(req.uri().startsWith(WEBHDFS_PREFIX));
    // 解析请求查询参数
    QueryStringDecoder queryString = new QueryStringDecoder(req.uri());
    params = new ParameterParser(queryString, conf);
    // 获取请求对应用户身份信息
    DataNodeUGIProvider ugiProvider = new DataNodeUGIProvider(params);
    ugi = ugiProvider.ugi();
    path = params.path();

    // 注入请求携带的委托令牌
    injectToken();
    // 在对应用户身份下执行请求处理
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          handle(ctx, req);
        } finally {
          // 获取客户端IP地址用于日志记录
          String host = null;
          try {
            host = ((InetSocketAddress)ctx.channel().remoteAddress()).
                getAddress().getHostAddress();
          } catch (Exception e) {
            LOG.warn("Error retrieving hostname: ", e);
            host = "unknown";
          }
          // 记录WebHDFS访问日志
          REQLOG.info(host + " " + req.method() + " "  + req.uri() + " " +
              getResponseCode());
        }
        return null;
      }
    });
  }

  /**
   * 获取当前请求响应状态码，用于访问日志记录。
   * @return 响应状态码，未生成响应时返回500错误码
   */
  int getResponseCode() {
    return (resp == null) ? INTERNAL_SERVER_ERROR.code() :
        resp.status().code();
  }

  /**
   * 根据请求操作和HTTP方法路由到对应处理逻辑。
   * @param ctx Netty通道上下文
   * @param req HTTP请求对象
   * @throws IOException IO异常
   * @throws URISyntaxException URI语法异常
   */
  public void handle(ChannelHandlerContext ctx, HttpRequest req)
    throws IOException, URISyntaxException {
    String op = params.op();
    HttpMethod method = req.method();
    if (PutOpParam.Op.CREATE.name().equalsIgnoreCase(op)
      && method == PUT) {
      // 处理创建文件写请求
      onCreate(ctx);
    } else if (PostOpParam.Op.APPEND.name().equalsIgnoreCase(op)
      && method == POST) {
      // 处理文件追加请求
      onAppend(ctx);
    } else if (GetOpParam.Op.OPEN.name().equalsIgnoreCase(op)
      && method == GET) {
      // 处理读取文件请求
      onOpen(ctx);
    } else if(GetOpParam.Op.GETFILECHECKSUM.name().equalsIgnoreCase(op)
      && method == GET) {
      // 处理获取文件校验和请求
      onGetFileChecksum(ctx);
    } else if(PutOpParam.Op.CREATE.name().equalsIgnoreCase(op)
        && method == OPTIONS) {
      // 处理创建文件的CORS预检请求
      allowCORSOnCreate(ctx);
    } else {
      throw new IllegalArgumentException("Invalid operation " + op);
    }
  }

  @Override
  /**
   * 处理请求处理过程中抛出的异常，返回错误响应并关闭连接。
   */
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.debug("Error ", cause);
    // 将异常包装为HTTP错误响应
    resp = ExceptionHandler.exceptionCaught(cause);
    resp.headers().set(CONNECTION, CLOSE);
    // 发送响应后关闭连接
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * 处理创建文件写请求，初始化DFS客户端和输出流，替换当前处理器为写处理器处理后续数据。
   * @param ctx Netty通道上下文
   * @throws IOException IO异常
   * @throws URISyntaxException URI语法异常
   */
  private void onCreate(ChannelHandlerContext ctx)
    throws IOException, URISyntaxException {
    // 发送100 Continue响应告诉客户端可以发送请求体
    writeContinueHeader(ctx);

    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();
    final short replication = params.replication();
    final long blockSize = params.blockSize();
    final FsPermission unmaskedPermission = params.unmaskedPermission();
    // 计算最终权限，处理umask掩码
    final FsPermission permission = unmaskedPermission == null ?
        params.permission() :
        FsCreateModes.create(params.permission(), unmaskedPermission);
    final boolean createParent = params.createParent();

    EnumSet<CreateFlag> flags = params.createFlag();
    if (flags.equals(EMPTY_CREATE_FLAG)) {
      // 根据是否覆盖设置创建标记
      flags = params.overwrite() ?
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
          : EnumSet.of(CreateFlag.CREATE);
    } else {
      if(params.overwrite()) {
        flags.add(CreateFlag.OVERWRITE);
      }
    }

    // 创建指向目标NameNode的DFS客户端
    final DFSClient dfsClient = newDfsClient(nnId, confForCreate);
    // 创建文件并获取包装输出流
    OutputStream out = dfsClient.createWrappedOutputStream(dfsClient.create(
        path, permission, flags, createParent, replication, blockSize, null,
        bufferSize, null), null);

    resp = new DefaultHttpResponse(HTTP_1_1, CREATED);
    // 设置Location头指向新创建文件的HDFS URI
    final URI uri = new URI(HDFS_URI_SCHEME, nnId, path, null, null);
    resp.headers().set(LOCATION, uri.toString());
    resp.headers().set(CONTENT_LENGTH, 0);
    // 允许跨域访问
    resp.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");

    // 替换当前处理器为HdfsWriter，处理后续请求体写入
    ctx.pipeline().replace(this, HdfsWriter.class.getSimpleName(),
      new HdfsWriter(dfsClient, out, resp));
  }

  /**
   * 处理文件追加请求，初始化DFS客户端和追加输出流，替换处理器为写处理器处理后续数据。
   * @param ctx Netty通道上下文
   * @throws IOException IO异常
   */
  private void onAppend(ChannelHandlerContext ctx) throws IOException {
    // 发送100 Continue响应告诉客户端可以发送请求体
    writeContinueHeader(ctx);
    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();

    // 创建指向目标NameNode的DFS客户端
    DFSClient dfsClient = newDfsClient(nnId, conf);
    // 打开文件追加输出流
    OutputStream out = dfsClient.append(path, bufferSize,
        EnumSet.of(CreateFlag.APPEND), null, null);
    resp = new DefaultHttpResponse(HTTP_1_1, OK);
    resp.headers().set(CONTENT_LENGTH, 0);
    // 替换当前处理器为HdfsWriter处理后续数据写入
    ctx.pipeline().replace(this, HdfsWriter.class.getSimpleName(),
      new HdfsWriter(dfsClient, out, resp));
  }

  /**
   * 处理读取文件请求，读取指定范围数据并分块返回给客户端。
   * @param ctx Netty通道上下文
   * @throws IOException IO异常
   */
  private void onOpen(ChannelHandlerContext ctx) throws IOException {
    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();
    final long offset = params.offset();
    final long length = params.length();

    resp = new DefaultHttpResponse(HTTP_1_1, OK);
    HttpHeaders headers = resp.headers();
    // Allow the UI to access the file
    // 配置跨域访问允许前端读取文件数据
    headers.set(ACCESS_CONTROL_ALLOW_METHODS, GET);
    headers.set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    headers.set(CONTENT_TYPE, APPLICATION_OCTET_STREAM);
    headers.set(CONNECTION, CLOSE);

    // 创建指向目标NameNode的DFS客户端
    final DFSClient dfsclient = newDfsClient(nnId, conf);
    // 打开文件输入流并跳转到指定偏移量
    HdfsDataInputStream in = dfsclient.createWrappedInputStream(
      dfsclient.open(path, bufferSize, true));
    in.seek(offset);

    // 计算本次返回内容长度
    long contentLength = in.getVisibleLength() - offset;
    if (length >= 0) {
      contentLength = Math.min(contentLength, length);
    }
    final InputStream data;
    // 根据是否指定长度包装输入流，设置Content-Length头
    if (contentLength >= 0) {
      headers.set(CONTENT_LENGTH, contentLength);
      data = new LimitInputStream(in, contentLength);
    } else {
      data = in;
    }

    // 发送响应头，然后分块发送输入流数据，发送完成后关闭连接和客户端
    ctx.write(resp);
    ctx.writeAndFlush(new ChunkedStream(data) {
      @Override
      public void close() throws Exception {
        super.close();
        dfsclient.close();
      }
    }).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * 处理获取文件校验和请求，计算校验和并以JSON格式返回给客户端。
   * @param ctx Netty通道上下文
   * @throws IOException IO异常
   */
  private void onGetFileChecksum(ChannelHandlerContext ctx) throws IOException {
    MD5MD5CRC32FileChecksum checksum = null;
    final String nnId = params.namenodeId();
    DFSClient dfsclient = newDfsClient(nnId, conf);
    try {
      // 获取文件校验和
      checksum = dfsclient.getFileChecksum(path, Long.MAX_VALUE);
      dfsclient.close();
      dfsclient = null;
    } finally {
      // 清理DFS客户端资源
      IOUtils.cleanupWithLogger(LOG, dfsclient);
    }
    // 将校验和转换为JSON字节数组
    final byte[] js =
        JsonUtil.toJsonString(checksum).getBytes(StandardCharsets.UTF_8);
    // 创建