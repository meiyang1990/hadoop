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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 封装Docker exec命令及其命令行参数，用于在运行中的Docker容器内执行指定命令
 * 是YARN NodeManager对Docker容器执行exec操作的命令构建器
 */
public class DockerExecCommand extends DockerCommand {
  private static final String EXEC_COMMAND = "exec";
  // 存储用户环境变量
  private final Map<String, String> userEnv;

  /**
   * 构造Docker exec命令对象，指定目标容器ID
   * @param containerId 目标Docker容器ID
   */
  public DockerExecCommand(String containerId) {
    super(EXEC_COMMAND);
    super.addCommandArguments("name", containerId);
    this.userEnv = new LinkedHashMap<String, String>();
  }

  /**
   * 设置交互式模式，允许用户与exec执行的命令交互
   * @return 当前命令对象，支持链式调用
   */
  public DockerExecCommand setInteractive() {
    super.addCommandArguments("interactive", "true");
    return this;
  }

  /**
   * 分配伪终端TTY，支持交互式终端访问
   * @return 当前命令对象，支持链式调用
   */
  public DockerExecCommand setTTY() {
    super.addCommandArguments("tty", "true");
    return this;
  }

  /**
   * 设置容器内需要执行的命令及参数
   * @param overrideCommandWithArgs 待执行的命令和参数列表
   * @return 当前命令对象，支持链式调用
   */
  public DockerExecCommand setOverrideCommandWithArgs(
      List<String> overrideCommandWithArgs) {
    for(String override: overrideCommandWithArgs) {
      super.addCommandArguments("launch-command", override);
    }
    return this;
  }

  @Override
  public Map<String, List<String>> getDockerCommandWithArguments() {
    return super.getDockerCommandWithArguments();
  }

}