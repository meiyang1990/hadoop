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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

/**
 * 默认Linux容器运行时实现，通过PrivilegedOperationExecutor调用原生container-executor二进制，
 * 以标准原生进程模型启动YARN容器进程。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultLinuxContainerRuntime.class);
  // 临时命令文件前缀
  private static final String TMP_FILE_PREFIX = "yarn.";
  // 临时命令文件后缀
  private static final String TMP_FILE_SUFFIX = ".cmd";
  // 特权操作执行器实例，用于执行需要root权限的操作
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  // Hadoop配置对象
  private Configuration conf;

  /**
   * 使用指定的特权操作执行器构造默认Linux容器运行时实例。
   *
   * @param privilegedOperationExecutor 特权操作执行器实例
   */
  public DefaultLinuxContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;
  }

  @Override
  /**
   * 检查是否请求使用默认运行时
   */
  public boolean isRuntimeRequested(Map<String, String> env) {
    String type = env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);
    if (type == null) {
      // 从配置获取默认运行时类型
      type = conf.get(YarnConfiguration.LINUX_CONTAINER_RUNTIME_TYPE);
    }
    // 未指定类型或指定为default则使用本运行时
    return type == null || type.isEmpty() || type.equals("default");
  }

  @Override
  /**
   * 初始化运行时，保存配置信息
   */
  public void initialize(Configuration conf, Context nmContext)
      throws ContainerExecutionException {
    this.conf = conf;
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    //nothing to do here at the moment.
  }

  @Override
  /**
   * 启动容器，通过container-executor执行启动操作
   */
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    // 创建启动容器特权操作
    PrivilegedOperation launchOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.LAUNCH_CONTAINER);

    // 从运行时上下文获取所有必需参数并添加到操作参数
    launchOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation.
            RunAsUserCommand.LAUNCH_CONTAINER.getValue()),
        ctx.getExecutionAttribute(APPID),
        ctx.getExecutionAttribute(CONTAINER_ID_STR),
        ctx.getExecutionAttribute(CONTAINER_WORK_DIR).toString(),
        ctx.getExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH).toUri()
            .getPath(),
        ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH).toUri().getPath());
    // 获取HTTPS密钥库路径
    Path keystorePath = ctx.getExecutionAttribute(NM_PRIVATE_KEYSTORE_PATH);
    // 获取HTTPS信任库路径
    Path truststorePath = ctx.getExecutionAttribute(NM_PRIVATE_TRUSTSTORE_PATH);
    if (keystorePath != null && truststorePath != null) {
      // 两个都存在则启用HTTPS
      launchOp.appendArgs("--https",
          keystorePath.toUri().getPath(),
          truststorePath.toUri().getPath());
    } else {
      // 否则使用HTTP
      launchOp.appendArgs("--http");
    }
    // 添加PID文件路径、本地目录、日志目录、资源选项参数
    launchOp.appendArgs(ctx.getExecutionAttribute(PID_FILE_PATH).toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            ctx.getExecutionAttribute(LOCAL_DIRS)),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            ctx.getExecutionAttribute(LOG_DIRS)),
        ctx.getExecutionAttribute(RESOURCES_OPTIONS));

    // 获取流量控制命令文件路径
    String tcCommandFile = ctx.getExecutionAttribute(TC_COMMAND_FILE);

    if (tcCommandFile != null) {
      // 存在则添加到参数
      launchOp.appendArgs(tcCommandFile);
    }

    // 关闭本操作失败日志，由上层调用者决定是否处理失败
    launchOp.disableFailureLogging();

    // 从上下文获取容器启动前缀命令
    //List<String> -> stored as List -> fetched/converted to List<String>
    //we can't do better here thanks to type-erasure
    @SuppressWarnings("unchecked")
    List<String> prefixCommands = (List<String>) ctx.getExecutionAttribute(
        CONTAINER_LAUNCH_PREFIX_COMMANDS);

    try {
      // 执行特权启动操作
      privilegedOperationExecutor.executePrivilegedOperation(prefixCommands,
            launchOp, null, null, false, false);
    } catch (PrivilegedOperationException e) {
      // 启动失败封装异常抛出
      throw new ContainerExecutionException("Launch container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  /**
   * 重新启动容器，复用启动逻辑
   */
  public void relaunchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    launchContainer(ctx);
  }

  @Override
  /**
   * 向容器进程发送信号
   */
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    // 创建发送信号特权操作
    PrivilegedOperation signalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER);

    // 添加用户、PID、信号参数
    signalOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation.RunAsUserCommand
            .SIGNAL_CONTAINER.getValue()),
        ctx.getExecutionAttribute(PID),
        Integer.toString(ctx.getExecutionAttribute(SIGNAL).getValue()));

    // 关闭本操作失败日志，由上层调用者决定是否处理失败
    signalOp.disableFailureLogging();

    try {
      // 获取特权操作执行器实例
      PrivilegedOperationExecutor executor = PrivilegedOperationExecutor
          .getInstance(conf);
      // 执行发送信号操作
      executor.executePrivilegedOperation(null,
          signalOp, null, null, false, false);
    } catch (PrivilegedOperationException e) {
      //Don't log the failure here. Some kinds of signaling failures are
      // acceptable. Let the calling executor decide what to do.
      // 信号发送失败封装异常抛出
      throw new ContainerExecutionException("Signal container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

  }

  @Override
  /**
   * 获取本节点IP和主机名
   */
  public String[] getIpAndHost(Container container) {
    return ContainerExecutor.getLocalIpAndHost(container);
  }

  @Override
  /**
   * 默认运行时不暴露端口，返回null
   */
  public String getExposedPorts(Container container) {
    return null;
  }

  @Override
  /**
   * 在容器内执行交互式命令，返回IO流对用于交互
   */
  public IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException {
    IOStreamPair output;
    try {
      // 创建执行容器命令特权操作
      PrivilegedOperation privOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.EXEC_CONTAINER);
      // 将命令写入临时文件供container-executor读取
      String commandFile = writeCommandToTempFile(ctx);
      privOp.appendArgs(commandFile);
      // 关闭失败日志
      privOp.disableFailureLogging();
      // 执行交互式操作获取IO流对
      output =
          privilegedOperationExecutor.executePrivilegedInteractiveOperation(
              null, privOp);
    } catch (PrivilegedOperationException e) {
      // 执行失败封装异常抛出
      throw new ContainerExecutionException(
          "Execute container interactive shell failed", e.getExitCode(),
          e.getOutput(), e.getErrorOutput());
    } catch (InterruptedException ie) {
      // 中断异常处理
      LOG.warn("InterruptedException executing command: ", ie);
      throw new ContainerExecutionException(ie.getMessage());
    }
    // 返回IO流对
    return output;
  }

  /**
   * 将交互式执行命令写入临时配置文件，供container-executor读取执行
   * @param ctx 容器执行上下文
   * @return 临时命令文件路径
   * @throws ContainerExecutionException 写入失败抛出异常
   */
  private String writeCommandToTempFile(ContainerExecContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    File cmdDir = null;
    // 获取应用ID
    String appId = container.getContainerId().getApplicationAttemptId()
        .getApplicationId().toString();
    // 获取容器ID
    String containerId = container.getContainerId().toString();
    String filePrefix = containerId.toString();
    try {
      // 获取NM私有目录下写入命令文件的路径
      String cmdDirPath = ctx.getLocalDirsHandlerService().getLocalPathForWrite(
          ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR +
          appId + Path.SEPARATOR + filePrefix + Path.SEPARATOR).toString();
      cmdDir = new File(cmdDirPath);
      // 创建目录，不存在且创建失败抛出异常
      if (!cmdDir.mkdirs() && !cmdDir.exists()) {
        throw new IOException("Cannot create container private directory "
            + cmdDir);
      }
      // 创建临时命令文件
      File commandFile = File.createTempFile(TMP_FILE_PREFIX + filePrefix,
          TMP_FILE_SUFFIX, cmdDir);
      // 打开写入流，自动关闭
      try (
              Writer writer = new OutputStreamWriter(
              new FileOutputStream(commandFile.toString()), StandardCharsets.UTF_8);
              PrintWriter printWriter = new PrintWriter(writer);
      ) {
        // 初始化配置项Map
        Map<String, List<String>> cmd = new HashMap<String, List<String>>();
        // 配置执行类型为exec
        List<String> exec = new ArrayList<String>();
        exec.add("exec");
        cmd.put("command", exec);
        // 配置执行用户
        List<String> user = new ArrayList<String>();
        user.add(container.getUser());
        cmd.put("user", user);
        // 配置启动命令为交互式shell
        List<String> commands = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        sb.append("/bin/");
        sb.append(ctx.getShell());
        commands.add(sb.toString());
        commands.add("-ir");
        cmd.put("launch-command", commands);
        // 配置工作目录为容器工作目录
        List<String> workdir = new ArrayList<String>();
        workdir.add(container.getWorkDir());
        cmd.put("workdir", workdir);
        // 写入配置文件头
        printWriter.println("[command-execution]");
        // 遍历写入所有配置项
        for (Map.Entry<String, List<String>> entry :
            cmd.entrySet()) {
          // 检查键中不包含等号
          if (entry.getKey().contains("=")) {
            throw new ContainerExecutionException(
                "'=' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + entry.getValue());
          }
          // 检查值中不包含换行符
          if (entry.getValue().contains("\n")) {
            throw new ContainerExecutionException(
                "'\\n' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + entry.getValue());
          }
          // 写入配置项，值用逗号分隔
          printWriter.println("  " + entry.getKey() + "=" + StringUtils
              .join(",", entry.getValue()));
        }
        // 返回临时命令文件路径
        return commandFile.toString();
      }
    } catch (IOException e) {
      // 写入失败处理
      LOG.warn("Unable to write command to " + cmdDir);
      throw new ContainerExecutionException(e);
    }
  }
}