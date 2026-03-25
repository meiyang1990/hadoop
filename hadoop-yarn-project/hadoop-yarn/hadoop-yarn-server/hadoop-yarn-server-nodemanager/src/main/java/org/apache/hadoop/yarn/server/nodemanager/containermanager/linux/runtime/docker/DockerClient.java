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

/**
 * YARN NodeManager Docker 客户端工具类，负责生成Docker命令执行所需的配置文件，
 * 供NodeManager调用Docker启动容器时使用。
 * 属于YARN容器运行时模块，用于支持Docker容器化运行场景。
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DockerClient {
  private static final Logger LOG =
       LoggerFactory.getLogger(DockerClient.class);
  // Docker临时文件前缀
  private static final String TMP_FILE_PREFIX = "docker.";
  // Docker命令文件后缀
  private static final String TMP_FILE_SUFFIX = ".cmd";
  // Docker环境变量文件后缀
  private static final String TMP_ENV_FILE_SUFFIX = ".env";

  /**
   * 将Docker运行命令的环境变量写入临时文件，供Docker执行时读取。
   * @param cmd Docker运行命令对象，包含环境变量信息
   * @param filePrefix 临时文件名前缀，通常使用容器ID
   * @param cmdDir 临时文件存放目录
   * @return 环境变量临时文件的绝对路径
   * @throws IOException 写入文件失败时抛出异常
   */
  private String writeEnvFile(DockerRunCommand cmd, String filePrefix,
      File cmdDir) throws IOException {
    // 创建环境变量临时文件
    File dockerEnvFile = File.createTempFile(TMP_FILE_PREFIX + filePrefix,
        TMP_ENV_FILE_SUFFIX, cmdDir);
    // 自动关闭流，写入环境变量
    try (
            Writer envWriter = new OutputStreamWriter(
            new FileOutputStream(dockerEnvFile), StandardCharsets.UTF_8);
            PrintWriter envPrintWriter = new PrintWriter(envWriter);
    ) {
      // 逐行写入环境变量，格式为key=value
      for (Map.Entry<String, String> entry : cmd.getEnv()
          .entrySet()) {
        envPrintWriter.println(entry.getKey() + "=" + entry.getValue());
      }
      return dockerEnvFile.getAbsolutePath();
    }
  }

  /**
   * 将Docker命令写入NodeManager本地临时文件，供后续执行Docker命令使用。
   * 文件中包含所有Docker参数和环境变量文件路径，由NodeManager执行脚本读取调用。
   * @param cmd Docker命令对象，包含子命令和参数
   * @param containerId 目标容器ID，用于生成文件名和目录
   * @param nmContext NodeManager上下文，用于获取本地目录路径
   * @return 生成的Docker命令临时文件的绝对路径
   * @throws ContainerExecutionException 创建目录或写入文件失败时抛出异常
   */
  public String writeCommandToTempFile(DockerCommand cmd,
      ContainerId containerId, Context nmContext)
      throws ContainerExecutionException {
    String filePrefix = containerId.toString();
    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    File dockerCommandFile;
    File cmdDir = null;

    // 检查NodeManager上下文有效性
    if(nmContext == null || nmContext.getLocalDirsHandler() == null) {
      throw new ContainerExecutionException(
          "Unable to write temporary docker command");
    }

    try {
      // 构造容器私有目录路径，位于NM本地私有目录下按应用/容器分层
      String cmdDirPath = nmContext.getLocalDirsHandler().getLocalPathForWrite(
          ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR +
          appId + Path.SEPARATOR + filePrefix + Path.SEPARATOR).toString();
      cmdDir = new File(cmdDirPath);
      // 创建容器私有目录，创建失败且目录不存在则抛出异常
      if (!cmdDir.mkdirs() && !cmdDir.exists()) {
        throw new IOException("Cannot create container private directory "
            + cmdDir);
      }
      // 创建Docker命令临时文件
      dockerCommandFile = File.createTempFile(TMP_FILE_PREFIX + filePrefix,
          TMP_FILE_SUFFIX, cmdDir);
      // 自动关闭流，写入命令配置
      try (
        Writer writer = new OutputStreamWriter(
              new FileOutputStream(dockerCommandFile.toString()), StandardCharsets.UTF_8);
        PrintWriter printWriter = new PrintWriter(writer);
      ) {
        // 写入文件头标记
        printWriter.println("[docker-command-execution]");
        // 遍历写入所有Docker命令参数
        for (Map.Entry<String, List<String>> entry :
            cmd.getDockerCommandWithArguments().entrySet()) {
          // 校验参数key不包含等号，避免解析错误
          if (entry.getKey().contains("=")) {
            throw new ContainerExecutionException(
                "'=' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + entry.getValue());
          }
          // 将多值参数用逗号拼接成字符串
          String value = StringUtils.join(",", entry.getValue());
          // 校验参数不包含换行，避免解析错误
          if (value.contains("\n")) {
            throw new ContainerExecutionException(
                "'\\n' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + value);
          }
          // 写入参数，格式缩进 key=value
          printWriter.println("  " + entry.getKey() + "=" + value);
        }
        // 如果是Docker run命令且包含环境变量，写入环境变量文件路径
        if (cmd instanceof DockerRunCommand) {
          DockerRunCommand runCommand = (DockerRunCommand) cmd;
          if (runCommand.containsEnv()) {
            String path = writeEnvFile(runCommand, filePrefix, cmdDir);
            printWriter.println("  environ=" + path);
          }
        }
        // 返回命令文件绝对路径
        return dockerCommandFile.toString();
      }
    } catch (IOException e) {
      LOG.warn("Unable to write docker command to " + cmdDir);
      throw new ContainerExecutionException(e);
    }
  }
}