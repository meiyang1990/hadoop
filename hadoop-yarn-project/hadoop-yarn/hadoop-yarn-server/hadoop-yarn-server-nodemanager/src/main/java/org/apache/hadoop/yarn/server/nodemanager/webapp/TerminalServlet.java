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
package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.servlet.DefaultServlet;

/**
 * NodeManager Web UI终端页面Servlet，提供基于Xterm.js的Web终端前端静态资源托管，支持通过WebSocket连接容器执行命令
 */
@WebServlet(urlPatterns="/terminal/*")
public class TerminalServlet extends DefaultServlet {

  /**
   * Servlet序列化版本ID
   */
  private static final long serialVersionUID = 1336699L;

  /**
   * 处理GET请求，针对模板文件特殊设置Content-Type后交给父类处理静态资源
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // 如果请求的是模板文件，设置正确的HTML响应类型
    if (request.getRequestURI().endsWith(".template")) {
      response.setHeader("Content-Type", "text/html;charset=utf-8");
    }
    // 交给DefaultServlet处理静态资源返回
    super.doGet(request, response);
  }
}