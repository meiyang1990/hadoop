// 这个文件已经全部加上中文注释
/*
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
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@InterfaceAudience.Private
@InterfaceStability.Unstable

/**
 * Docker子命令抽象基类，代表Docker的各类子命令（如run、load、inspect等）
 * 负责管理Docker命令的参数，并提供构建特权操作的能力，用于在NodeManager上执行Docker命令
 */
public abstract class DockerCommand {
  private final String command;
  private final Map<String, List<String>> commandArguments;

  /**
   * 构造Docker命令对象，初始化命令名称和参数存储
   * @param command Docker子命令名称
   */
  protected DockerCommand(String command) {
    String dockerCommandKey = "docker-command";
    this.command = command;
    this.commandArguments = new TreeMap<>();
    commandArguments.put(dockerCommandKey, new ArrayList<>());
    commandArguments.get(dockerCommandKey).add(command);
  }

  /**
   * 获取当前Docker子命令名称
   * @return Docker子命令字符串，如"run"
   */
  public final String getCommandOption() {
    return this.command;
  }

  /**
   * 添加命令参数，仅允许子类调用
   * @param key 参数键名
   * @param value 参数值
   */
  protected final void addCommandArguments(String key, String value) {
    List<String> list = commandArguments.get(key);
    if (list != null) {
      list.add(value);
      return;
    }
    list = new ArrayList<>();
    list.add(value);
    this.commandArguments.put(key, list);
  }

  /**
   * 获取不可修改的Docker命令参数映射
   * @return 命令参数映射表
   */
  public Map<String, List<String>> getDockerCommandWithArguments() {
    return Collections.unmodifiableMap(commandArguments);
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder(this.command);
    for (Map.Entry<String, List<String>> entry : commandArguments.entrySet()) {
      ret.append(" ").append(entry.getKey());
      ret.append("=").append(StringUtils.join(",", entry.getValue()));
    }
    return ret.toString();
  }

  /**
   * 设置Docker客户端配置目录，用于指定Docker客户端的认证配置
   * Docker要求该目录下包含config.json文件，通常由docker login生成
   * @param clientConfigDir Docker客户端配置目录路径
   */
  public void setClientConfigDir(String clientConfigDir) {
    if (clientConfigDir != null) {
      addCommandArguments("docker-config", clientConfigDir);
    }
  }

  /**
   * 准备用于调用container-executor的特权操作对象
   * 将Docker命令写入临时文件，通过container-executor以特权身份执行
   * @param dockerCommand 待执行的Docker命令
   * @param containerName 容器ID字符串
   * @param env 环境变量映射
   * @param nmContext NodeManager上下文对象
   * @return 准备好的特权操作对象
   * @throws ContainerExecutionException 容器执行异常
   */
  public PrivilegedOperation preparePrivilegedOperation(
      DockerCommand dockerCommand, String containerName, Map<String,
      String> env, Context nmContext)
      throws ContainerExecutionException {
    DockerClient dockerClient = new DockerClient();
    String commandFile =
        dockerClient.writeCommandToTempFile(dockerCommand,
        ContainerId.fromString(containerName),
        nmContext);
    PrivilegedOperation dockerOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.RUN_DOCKER_CMD);
    dockerOp.appendArgs(commandFile);
    return dockerOp;
  }
}