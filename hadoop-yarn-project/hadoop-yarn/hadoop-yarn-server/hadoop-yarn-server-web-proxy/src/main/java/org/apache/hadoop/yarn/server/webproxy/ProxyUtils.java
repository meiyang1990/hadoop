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

package org.apache.hadoop.yarn.server.webproxy;

import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.EnumSet;

/**
 * YARN Web 代理通用工具类，提供页面生成、重定向、错误响应等通用能力
 */
public class ProxyUtils {
  private static final Logger LOG = LoggerFactory.getLogger(
      ProxyUtils.class);
  public static final String E_HTTP_HTTPS_ONLY =
      "This filter only works for HTTP/HTTPS";
  public static final String LOCATION = "Location";

  /**
   * Hamltet 标签占位类，用于结束标签链
   */
  public static class __ implements Hamlet.__ {
    //Empty
  }

  /**
   * 自定义HTML页面生成类，基于Hamlet框架构造代理服务响应页面
   */
  public static class Page extends Hamlet {
    Page(PrintWriter out) {
      super(out, 0, false);
    }

    /**
     * 创建根HTML标签
     * @return 根HTML标签实例
     */
    public HTML<ProxyUtils.__> html() {
      return new HTML<>("html", null, EnumSet.of(EOpt.ENDTAG));
    }
  }
  
  /**
   * 发送HTTP重定向响应，支持REST全功能，返回带跳转链接的HTML页面
   * <p>
   * 方法结束后会关闭响应输出流
   * @param request  HTTP请求对象，包含请求方法等信息
   * @param response HTTP响应对象，用于输出重定向结果
   * @param target   重定向目标URL（未编码）
   * @throws IOException 输出响应失败时抛出
   */
  public static void sendRedirect(HttpServletRequest request,
      HttpServletResponse response,
      String target)
      throws IOException {
    LOG.debug("Redirecting {} {} to {}",
          request.getMethod(), 
          request.getRequestURI(),
          target);
    // 编码重定向URL
    String location = response.encodeRedirectURL(target);
    // 设置302 FOUND状态码
    response.setStatus(HttpServletResponse.SC_FOUND);
    // 设置Location重定向头
    response.setHeader(LOCATION, location);
    // 设置内容类型为HTML
    response.setContentType(MimeType.HTML);
    PrintWriter writer = response.getWriter();
    // 生成带跳转链接的HTML页面
    Page p = new Page(writer);
    p.html()
        .head().title("Moved").__()
        .body()
        .h1("Moved")
        .div()
          .__("Content has moved ")
          .a(location, "here").__()
        .__().__();
    writer.close();
  }


  /**
   * 输出404 Not Found错误响应页面
   * @param resp HTTP响应对象
   * @param message 404页面显示的错误信息
   * @throws IOException 输出响应失败时抛出
   */
  public static void notFound(HttpServletResponse resp, String message)
      throws IOException {
    // 设置404状态码
    resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
    // 设置内容类型为HTML
    resp.setContentType(MimeType.HTML);
    // 生成仅包含错误信息的简单HTML页面
    Page p = new Page(resp.getWriter());
    p.html().
        h1(message).
        __();
  }

  /**
   * 校验并拒绝非HTTP请求，仅保留HTTP/HTTPS请求
   * @param req 入站请求对象
   * @throws ServletException 请求不是HTTP请求时抛出
   */
  public static void rejectNonHttpRequests(ServletRequest req) throws
      ServletException {
    if (!(req instanceof HttpServletRequest)) {
      throw new ServletException(E_HTTP_HTTPS_ONLY);
    }
  }
}