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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ShellContainerCommand;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通过NodeManager连接到容器执行器的交互式命令Shell WebSocket服务，
 * 支持在Web界面中直接操作容器内的命令行环境。
 */
@InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce", "YARN" })
@InterfaceStability.Unstable

@WebSocket
public class ContainerShellWebSocket {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerShellWebSocket.class);
  // NodeManager全局上下文实例
  private static Context nmContext;

  private final ContainerExecutor exec;
  // 容器进程输入输出流对，in是进程输出流，out是进程输入流
  private IOStreamPair pair;

  /**
   * 构造函数，从NodeManager上下文获取容器执行器实例
   */
  public ContainerShellWebSocket() {
    exec = nmContext.getContainerExecutor();
  }

  /**
   * 初始化WebSocket服务，保存NodeManager上下文
   * @param nm NodeManager上下文实例
   */
  public static void init(Context nm) {
    ContainerShellWebSocket.nmContext = nm;
  }

  /**
   * 处理WebSocket客户端发来的文本消息，转发输入并读取输出返回给客户端
   * @param session WebSocket会话
   * @param message 客户端发来的消息内容
   * @throws IOException IO异常
   */
  @OnWebSocketMessage
  public void onText(Session session, String message) throws IOException {

    try {
      byte[] buffer = new byte[4000];
      if (session.isOpen()) {
        // 心跳包不转发输入
        if (!message.equals("1{}")) {
          // 将用户按键输入写入容器进程输入流
          byte[] payload;
          payload = message.getBytes(StandardCharsets.UTF_8);
          if (payload != null) {
            pair.out.write(payload);
            pair.out.flush();
          }
        }
        // 读取容器进程输出，返回给前端
        int no = pair.in.available();
        pair.in.read(buffer, 0, Math.min(no, buffer.length));
        // 将换行符转为CRLF格式适配浏览器终端
        String formatted = new String(buffer, StandardCharsets.UTF_8)
            .replaceAll("\n", "\r\n");
        session.getRemote().sendString(formatted);
      }
    } catch (IOException e) {
      // 异常时关闭连接
      onClose(session, 1001, "Shutdown");
    }

  }

  /**
   * WebSocket连接建立时触发，初始化容器交互式Shell连接
   * @param session WebSocket会话
   */
  @OnWebSocketConnect
  public void onConnect(Session session) {
    try {
      // 从请求URI解析容器ID和命令类型
      URI containerURI = session.getUpgradeRequest().getRequestURI();
      String command = "bash";
      String[] containerPath = containerURI.getPath().split("/");
      String cId = containerPath[2];
      if (containerPath.length==4) {
        // 匹配指定的Shell命令
        for (ShellContainerCommand c : ShellContainerCommand.values()) {
          if (c.name().equalsIgnoreCase(containerPath[3])) {
            command = containerPath[3].toLowerCase();
          }
        }
      }
      // 根据ID获取容器实例
      Container container = nmContext.getContainers().get(ContainerId
          .fromString(cId));
      // 权限校验，不通过则关闭连接
      if (!checkAuthorization(session, container)) {
        session.close(1008, "Forbidden");
        return;
      }
      // 非安全模式校验，不符合要求则关闭连接
      if (checkInsecureSetup()) {
        session.close(1003, "Nonsecure mode is unsupported.");
        return;
      }
      LOG.info(session.getRemoteAddress().getHostString() + " connected!");
      LOG.info(
          "Making interactive connection to running docker container with ID: "
              + cId);
      // 构建容器执行上下文
      ContainerExecContext execContext = new ContainerExecContext
          .Builder()
          .setContainer(container)
          .setNMLocalPath(nmContext.getLocalDirsHandler())
          .setShell(command)
          .build();
      // 启动交互式Shell，获取进程输入输出流
      pair = exec.execContainer(execContext);
    } catch (Exception e) {
      LOG.error("Failed to establish WebSocket connection with Client", e);
    }

  }

  /**
   * WebSocket连接关闭时触发，清理进程流资源
   * @param session WebSocket会话
   * @param status 关闭状态码
   * @param reason 关闭原因
   */
  @OnWebSocketClose
  public void onClose(Session session, int status, String reason) {
    try {
      LOG.info(session.getRemoteAddress().getHostString() + " closed!");
      // 发送exit命令退出Shell
      String exit = "exit\r\n";
      pair.out.write(exit.getBytes(StandardCharsets.UTF_8));
      pair.out.flush();
      // 关闭输入输出流
      pair.in.close();
      pair.out.close();
    } catch (IOException e) {
    } finally {
      // 关闭WebSocket会话
      session.close();
    }
  }

  /**
   * 校验当前用户是否有权限访问目标容器
   * 仅容器提交用户本身或集群管理员允许访问
   * @param session websocket会话
   * @param container 待访问的容器实例
   * @return true表示允许访问，false表示拒绝
   * @throws IOException IO异常
   */
  protected boolean checkAuthorization(Session session, Container container)
      throws IOException {
    boolean authorized = true;
    String user = "";
    // 安全模式下从请求Principal提取用户名
    if (UserGroupInformation.isSecurityEnabled()) {
      user = new HadoopKerberosName(session.getUpgradeRequest()
          .getUserPrincipal().getName()).getShortName();
    } else {
      // 非安全模式下从请求参数获取用户名
      Map<String, List<String>> parameters = session.getUpgradeRequest()
          .getParameterMap();
      if (parameters.containsKey("user.name")) {
        List<String> users = parameters.get("user.name");
        user = users.get(0);
      }
    }
    boolean isAdmin = false;
    // ACL开启时检查是否是集群管理员
    if (nmContext.getApplicationACLsManager().areACLsEnabled()) {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
      isAdmin = nmContext.getApplicationACLsManager().isAdmin(ugi);
    }
    // 既不是容器所有者也不是管理员则拒绝访问
    String containerUser = container.getUser();
    if (!user.equals(containerUser) && !isAdmin) {
      authorized = false;
    }
    return authorized;
  }

  /**
   * 检查非安全模式是否允许启用交互式Shell
   * 非安全模式默认限制使用，仅开启Kerberos安全认证后允许
   * @return true表示不允许，false表示允许
   */
  private boolean checkInsecureSetup() {
    boolean kerberos = UserGroupInformation.isSecurityEnabled();
    boolean limitUsers = nmContext.getConf()
        .getBoolean(YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS, true);
    if (kerberos) {
      return false;
    }
    return limitUsers;
  }
}