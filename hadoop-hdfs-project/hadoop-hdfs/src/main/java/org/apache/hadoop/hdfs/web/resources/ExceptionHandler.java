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
package org.apache.hadoop.hdfs.web.resources;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;

import org.apache.hadoop.classification.VisibleForTesting;
import org.glassfish.jersey.server.ContainerException;
import org.glassfish.jersey.server.ParamException;
import org.glassfish.hk2.api.MultiException;

/**
 * HDFS Web REST API全局异常处理器
 * 捕获所有接口抛出的异常，统一转换为标准JSON格式响应，并根据异常类型设置对应HTTP状态码
 */
@Provider
public class ExceptionHandler implements ExceptionMapper<Exception> {
  public static final Logger LOG =
      LoggerFactory.getLogger(ExceptionHandler.class);

  /**
   * 从异常包装中提取最根本的根因异常
   * 特殊处理SecurityException嵌套InvalidToken再嵌套StandbyException的场景，返回原始StandbyException
   * @param e 原始待解包的异常
   * @return 提取后的根因异常
   */
  private static Exception toCause(Exception e) {
    final Throwable t = e.getCause();    
    if (e instanceof SecurityException) {
      // 处理HDFS-6475报告的安全异常嵌套场景：SecurityException -> InvalidToken -> StandbyException
      // 此时直接返回最内层的StandbyException，由异常处理器正确处理
      if (t != null && t instanceof InvalidToken) {
        final Throwable t1 = t.getCause();
        if (t1 != null && t1 instanceof StandbyException) {
          e = (StandbyException)t1;
        }
      }
    } else {
      // 非安全异常场景，如果存在异常包装，直接提取第一层cause作为处理目标
      if (t != null && t instanceof Exception) {
        e = (Exception)t;
      }
    }
    return e;
  }

  /** 注入当前请求的HttpServletResponse对象 */
  private @Context HttpServletResponse response;

  /**
   * 将异常转换为标准REST API响应
   * 对异常进行解包处理，根据异常类型匹配对应HTTP状态码，最后序列化为JSON返回
   * @param e 捕获到的异常
   * @return 构建好的标准响应对象
   */
  @Override
  public Response toResponse(Exception e) {
    // 开启trace日志时记录完整异常栈
    if (LOG.isTraceEnabled()) {
      LOG.trace("GOT EXCEPITION", e);
    }

    // 清空原有内容类型，由本处理器统一设置
    response.setContentType(null);

    // 异常转换与解包处理
    // 处理参数解析异常，转换为友好的非法参数异常
    if (e instanceof ParamException) {
      final ParamException paramexception = (ParamException)e;
      e = new IllegalArgumentException("Invalid value for webhdfs parameter \""
          + paramexception.getParameterName() + "\": "
          + e.getCause().getMessage(), e);
    }
    // 解包Jersey容器异常，提取根因
    if (e instanceof ContainerException) {
      e = toCause(e);
    }
    // 解包Hadoop RPC远程异常，提取实际服务端抛出的异常类型
    if (e instanceof RemoteException) {
      e = ((RemoteException)e).unwrapRemoteException();
    }

    // 解包安全异常，提取可能的根因
    if (e instanceof SecurityException) {
      e = toCause(e);
    }

    // 解包HK2多异常，提取根因
    if(e instanceof MultiException) {
      e = toCause(e);
    }
    
    // 根据异常类型匹配HTTP响应状态码
    final Response.Status s;
    if (e instanceof SecurityException) {
      s = Response.Status.FORBIDDEN;
    } else if (e instanceof AuthorizationException) {
      s = Response.Status.FORBIDDEN;
    } else if (e instanceof FileNotFoundException) {
      s = Response.Status.NOT_FOUND;
    } else if (e instanceof IOException) {
      s = Response.Status.FORBIDDEN;
    } else if (e instanceof UnsupportedOperationException) {
      s = Response.Status.BAD_REQUEST;
    } else if (e instanceof IllegalArgumentException) {
      s = Response.Status.BAD_REQUEST;
    } else if (e instanceof MultiException) {
      s = Response.Status.FORBIDDEN;
    } else {
      // 未识别的异常，记录警告日志，返回500错误
      LOG.warn("INTERNAL_SERVER_ERROR", e);
      s = Response.Status.INTERNAL_SERVER_ERROR;
    }
 
    // 将异常序列化为JSON字符串，返回JSON格式响应
    final String js = JsonUtil.toJsonString(e);
    return Response.status(s).type(MediaType.APPLICATION_JSON).entity(js).build();
  }
  
  /**
   * 测试用初始化方法，设置HttpServletResponse对象
   * @param response 测试使用的响应对象
   */
  @VisibleForTesting
  public void initResponse(HttpServletResponse response) {
    this.response = response;
  }
}