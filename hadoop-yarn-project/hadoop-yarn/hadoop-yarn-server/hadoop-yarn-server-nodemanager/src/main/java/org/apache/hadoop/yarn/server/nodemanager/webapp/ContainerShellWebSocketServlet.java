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

import javax.servlet.annotation.WebServlet;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

/**
 * 容器交互式Shell的WebSocket服务端Servlet，负责处理NodeManager节点上容器的终端交互WebSocket连接
 */
@WebServlet(urlPatterns="/container/container/*")
public class ContainerShellWebSocketServlet extends WebSocketServlet{

  /**
   * 配置WebSocket工厂，注册容器Shell消息处理类
   * @param factory WebSocket服务工厂实例
   */
  @Override
  public void configure(WebSocketServletFactory factory) {
    factory.register(ContainerShellWebSocket.class);
  }
}