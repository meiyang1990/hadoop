// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

/**
 * 封装Docker kill命令及其命令行参数，用于向Docker容器发送终止信号
 */
public class DockerKillCommand extends DockerCommand {
  private static final String KILL_COMMAND = "kill";

  /**
   * 构造指定容器的Docker kill命令对象
   * @param containerName 目标Docker容器名称
   */
  public DockerKillCommand(String containerName) {
    super(KILL_COMMAND);
    super.addCommandArguments("name", containerName);
  }

  /**
   * 设置要发送给Docker容器的终止信号
   *
   * @param signal  要发送给容器的信号
   * @return 设置完信号参数的DockerKillCommand对象
   */
  public DockerKillCommand setSignal(String signal) {
    super.addCommandArguments("signal", signal);
    return this;
  }
}