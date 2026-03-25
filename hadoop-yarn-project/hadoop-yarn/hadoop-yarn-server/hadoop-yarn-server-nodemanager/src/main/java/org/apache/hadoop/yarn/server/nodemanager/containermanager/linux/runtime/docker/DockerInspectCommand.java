// 这个文件已经全部加上中文注释
/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.Map;

/**
 * 文件：DockerInspectCommand.java
 * 所属模块：YARN NodeManager Docker容器运行时
 * 核心职责：封装Docker inspect命令及其参数构造逻辑，用于获取Docker容器元信息
 */
public class DockerInspectCommand extends DockerCommand {
  private static final String INSPECT_COMMAND = "inspect";
  private String commandArguments;

  /**
   * 构造指定容器的inspect命令
   * @param containerName 目标Docker容器名称
   */
  public DockerInspectCommand(String containerName) {
    super(INSPECT_COMMAND);
    super.addCommandArguments("name", containerName);
  }

  /**
   * 配置命令获取容器运行状态
   * @return 当前命令对象
   */
  public DockerInspectCommand getContainerStatus() {
    super.addCommandArguments("format", STATUS_TEMPLATE);
    this.commandArguments = String.format("--format=%s", STATUS_TEMPLATE);
    return this;
  }

  /**
   * 配置命令获取容器IP地址和主机名
   * @return 当前命令对象
   */
  public DockerInspectCommand getIpAndHost() {
    // Be sure to not use space in the argument, otherwise the
    // extract_values_delim method in container-executor binary
    // cannot parse the arguments correctly.
    super.addCommandArguments("format", "{{range(.NetworkSettings.Networks)}}"
        + "{{.IPAddress}},{{end}}{{.Config.Hostname}}");
    this.commandArguments = "--format={{range(.NetworkSettings.Networks)}}"
        + "{{.IPAddress}},{{end}}{{.Config.Hostname}}";
    return this;
  }

  /**
   * 配置命令按自定义模板获取指定信息
   * @param templates 自定义Go模板数组
   * @param delimiter 结果分隔符
   * @return 当前命令对象
   */
  public DockerInspectCommand get(String[] templates, char delimiter) {
    String format = StringUtils.join(delimiter, templates);
    super.addCommandArguments("format", format);
    this.commandArguments = String.format("--format=%s", format);
    return this;
  }

  /**
   * 生成特权操作对象，用于通过容器执行器执行inspect命令
   * @param dockerCommand 待执行的docker命令
   * @param containerName 目标容器名称
   * @param env 环境变量
   * @param nmContext NodeManager上下文
   * @return 构造完成的特权操作对象
   */
  @Override
  public PrivilegedOperation preparePrivilegedOperation(
      DockerCommand dockerCommand, String containerName, Map<String,
      String> env, Context nmContext) {
    PrivilegedOperation dockerOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.INSPECT_DOCKER_CONTAINER);
    dockerOp.appendArgs(commandArguments, containerName);
    return dockerOp;
  }

  // Go模板：获取容器状态
  public static final String STATUS_TEMPLATE = "{{.State.Status}}";
  // Go模板：获取容器停止信号
  public static final String STOPSIGNAL_TEMPLATE = "{{.Config.StopSignal}}";

  /**
   * 配置命令获取容器暴露端口信息
   * @return 当前命令对象
   */
  public DockerInspectCommand getExposedPorts() {
    super.addCommandArguments("format", "{{json .NetworkSettings.Ports}}");
    this.commandArguments = "--format={{json .NetworkSettings.Ports}}";
    return this;
  }

}