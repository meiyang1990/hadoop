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

/**
 * 封装Docker stop停止容器命令及其命令行参数，用于YARN NodeManager停止Docker容器场景
 */
public class DockerStopCommand extends DockerCommand {
  private static final String STOP_COMMAND = "stop";

  /**
   * 构造Docker stop停止命令，指定要停止的容器名称
   * @param containerName 要停止的Docker容器名称
   */
  public DockerStopCommand(String containerName) {
    super(STOP_COMMAND);
    super.addCommandArguments("name", containerName);
  }

  /**
   * 设置停止容器前的优雅等待超时时间
   * @param value 等待超时时间（秒）
   * @return 当前命令对象，支持链式调用
   */
  public DockerStopCommand setGracePeriod(int value) {
    super.addCommandArguments("time", Integer.toString(value));
    return this;
  }
}