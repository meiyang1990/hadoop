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
 * 封装Docker start命令及其命令行参数，用于在YARN NodeManager上启动已创建的Docker容器
 */
public class DockerStartCommand extends DockerCommand {
  // Docker start命令固定名称
  private static final String START_COMMAND = "start";

  /**
   * 构造Docker start命令对象，添加指定容器名称参数
   * @param containerName 要启动的Docker容器名称
   */
  public DockerStartCommand(String containerName) {
    super(START_COMMAND);
    super.addCommandArguments("name", containerName);
  }
}