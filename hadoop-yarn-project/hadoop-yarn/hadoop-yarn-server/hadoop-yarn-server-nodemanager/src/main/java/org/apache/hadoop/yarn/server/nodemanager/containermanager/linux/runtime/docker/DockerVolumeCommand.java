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

import java.util.regex.Pattern;

/**
 * Docker卷操作命令封装类，用于构造docker volume相关命令，具体命令说明参考docker volume --help。
 * 此类为YARN NodeManager管理Docker容器数据卷提供命令构造能力，支持创建、列出数据卷操作。
 */
public class DockerVolumeCommand extends DockerCommand {
  // docker volume主命令固定名称
  public static final String VOLUME_COMMAND = "volume";
  // volume create子命令固定名称
  public static final String VOLUME_CREATE_SUB_COMMAND = "create";
  // volume ls子命令固定名称
  public static final String VOLUME_LS_SUB_COMMAND = "ls";

  // Docker卷名称合法性校验正则表达式
  public static final Pattern VOLUME_NAME_PATTERN = Pattern.compile(
      "[a-zA-Z0-9][a-zA-Z0-9_.-]*");

  private String volumeName;
  private String driverName;
  private String subCommand;

  /**
   * 构造指定子命令的Docker卷操作命令对象。
   * @param subCommand 要执行的volume子命令(create/ls)
   */
  public DockerVolumeCommand(String subCommand) {
    super(VOLUME_COMMAND);
    this.subCommand = subCommand;
    super.addCommandArguments("sub-command", subCommand);
  }

  /**
   * 设置目标卷名称，添加对应命令参数。
   * @param volumeName 卷名称
   * @return 当前命令对象，支持链式调用
   */
  public DockerVolumeCommand setVolumeName(String volumeName) {
    super.addCommandArguments("volume", volumeName);
    this.volumeName = volumeName;
    return this;
  }

  /**
   * 设置卷驱动名称，添加对应命令参数。
   * @param driverName 驱动名称
   * @return 当前命令对象，支持链式调用
   */
  public DockerVolumeCommand setDriverName(String driverName) {
    super.addCommandArguments("driver", driverName);
    this.driverName = driverName;
    return this;
  }

  /**
   * 获取当前操作的卷名称。
   * @return 卷名称
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * 获取当前设置的卷驱动名称。
   * @return 驱动名称
   */
  public String getDriverName() {
    return driverName;
  }

  /**
   * 获取当前执行的volume子命令。
   * @return 子命令名称
   */
  public String getSubCommand() {
    return subCommand;
  }

  /**
   * 设置输出格式参数，用于volume ls命令自定义输出格式。
   * @param format 格式字符串
   * @return 当前命令对象，支持链式调用
   */
  public DockerVolumeCommand setFormat(String format) {
    super.addCommandArguments("format", format);
    return this;
  }

}