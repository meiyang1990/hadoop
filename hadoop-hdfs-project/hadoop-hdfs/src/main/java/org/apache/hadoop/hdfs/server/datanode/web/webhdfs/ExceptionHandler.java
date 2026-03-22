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

import org.glassfish.jersey.server.ParamException;
import org.glassfish.jersey.server.ContainerException;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.SecretManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.APPLICATION_JSON_UTF8;

/**
 * DataNode WebHDFS服务异常处理器，负责将各类异常转换为标准的Netty HTTP响应，
 * 统一处理异常类型转换、状态码映射和JSON格式响应生成。
 */
class ExceptionHandler {
  private static final Logger LOG = WebHdfsHandler.LOG;

  /**
   * 捕获处理WebHDFS请求过程中抛出的异常，将其转换为标准HTTP响应返回。
   * @param cause 请求处理过程中抛出的异常根因
   * @return 封装了异常信息的标准Netty FullHttp响应对象
   */
  static DefaultFullHttpResponse exceptionCaught(Throwable cause) {
    // 统一将Throwable转为Exception类型
    Exception e = cause instanceof Exception ? (Exception) cause : new Exception(cause);

    // 开启trace日志时记录异常堆栈
    if (LOG.isTraceEnabled()) {
      LOG.trace("GOT EXCEPTION", e);
    }

    // 异常类型转换处理
    if (e instanceof ParamException) {
      final ParamException paramexception = (ParamException)e;
      // 将Jersey参数异常转换为更友好的非法参数异常
      e = new IllegalArgumentException("Invalid value for webhdfs parameter \""
                                         + paramexception.getParameterName() + "\": "
                                         + e.getCause().getMessage(), e);
    } else if (e instanceof ContainerException || e instanceof SecurityException) {
      // 提取容器异常或安全异常的根因进行处理
      e = toCause(e);
    } else if (e instanceof RemoteException) {
      // 反序列化RPC远程异常为本地异常
      e = ((RemoteException)e).unwrapRemoteException();
    }

    // 根据异常类型映射对应的HTTP响应状态码
    final HttpResponseStatus s;
    if (e instanceof SecurityException) {
      s = FORBIDDEN;
    } else if (e instanceof AuthorizationException) {
      s = FORBIDDEN;
    } else if (e instanceof FileNotFoundException) {
      s = NOT_FOUND;
    } else if (e instanceof IOException) {
      s = FORBIDDEN;
    } else if (e instanceof UnsupportedOperationException) {
      s = BAD_REQUEST;
    } else if (e instanceof IllegalArgumentException) {
      s = BAD_REQUEST;
    } else {
      // 未分类异常记录警告日志，返回500错误
      LOG.warn("INTERNAL_SERVER_ERROR", e);
      s = INTERNAL_SERVER_ERROR;
    }

    // 将异常转换为JSON格式字节数组
    final byte[] js = JsonUtil.toJsonString(e).getBytes(StandardCharsets.UTF_8);
    // 创建完整HTTP响应对象
    DefaultFullHttpResponse resp =
      new DefaultFullHttpResponse(HTTP_1_1, s, Unpooled.wrappedBuffer(js));

    // 设置响应头
    resp.headers().set(CONTENT_TYPE, APPLICATION_JSON_UTF8);
    resp.headers().set(CONTENT_LENGTH, js.length);
    return resp;
  }

  /**
   * 提取异常的根因，针对安全异常做特殊处理（兼容HDFS-6475、HDFS-6588问题处理）。
   * @param e 待处理的原始异常
   * @return 处理后的根因异常
   */
  private static Exception toCause(Exception e) {
    final Throwable t = e.getCause();
    if (e instanceof SecurityException) {
      // 处理特殊场景：SecurityException包装InvalidToken，InvalidToken又包装StandbyException的情况
      // 需要提取最内层的StandbyException返回，以便正确处理主备切换场景
      if (t != null && t instanceof SecretManager.InvalidToken) {
        final Throwable t1 = t.getCause();
        if (t1 != null && t1 instanceof StandbyException) {
          e = (StandbyException)t1;
        }
      }
    } else {
      // 非安全异常直接提取第一层非空异常作为根因
      if (t != null && t instanceof Exception) {
        e = (Exception)t;
      }
    }
    return e;
  }

}