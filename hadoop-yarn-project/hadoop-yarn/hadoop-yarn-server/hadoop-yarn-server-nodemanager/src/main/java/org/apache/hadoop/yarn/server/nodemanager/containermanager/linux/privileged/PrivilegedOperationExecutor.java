// 这个文件已经全部加上中文注释
/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * YARN NodeManager上特权容器操作执行器，负责通过外部container-executor二进制文件执行需要root权限的操作
 * 包括cgroups管控、容器磁盘限额、网络管控等需要特权的Linux系统操作
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrivilegedOperationExecutor {
  private static final Logger LOG =
       LoggerFactory.getLogger(PrivilegedOperationExecutor
      .class);
  // 单例实例，双重检查锁定实现线程安全初始化
  private volatile static PrivilegedOperationExecutor instance;

  // container-executor二进制文件的绝对路径
  private String containerExecutorExe;

  /**
   * 从配置获取container-executor二进制文件的绝对路径，未配置则使用默认路径
   * @param conf YARN配置对象
   * @return container-executor可执行文件路径
   */
  public static String getContainerExecutorExecutablePath(Configuration conf) {
    String yarnHomeEnvVar =
        System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
    File hadoopBin = new File(yarnHomeEnvVar, "bin");
    String defaultPath =
        new File(hadoopBin, "container-executor").getAbsolutePath();
    return null == conf
        ? defaultPath
        : conf.get(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH,
        defaultPath);
  }

  private void init(Configuration conf) {
    containerExecutorExe = getContainerExecutorExecutablePath(conf);
  }

  private PrivilegedOperationExecutor(Configuration conf) {
    init(conf);
  }

  /**
   * 获取PrivilegedOperationExecutor单例实例，线程安全延迟初始化
   * @param conf YARN配置对象
   * @return 单例实例
   */
  public static PrivilegedOperationExecutor getInstance(Configuration conf) {
    if (instance == null) {
      synchronized (PrivilegedOperationExecutor.class) {
        if (instance == null) {
          instance = new PrivilegedOperationExecutor(conf);
        }
      }
    }

    return instance;
  }

  /**
   * 构造特权操作执行命令数组，拼接前缀命令、container-executor路径和操作参数
   * @param prefixCommands 前缀命令（如nice调整优先级）
   * @param operation 待执行的特权操作对象
   * @return 完整的执行命令数组
   */
  public String[] getPrivilegedOperationExecutionCommand(List<String>
      prefixCommands,
      PrivilegedOperation operation) {
    List<String> fullCommand = new ArrayList<String>();

    if (prefixCommands != null && !prefixCommands.isEmpty()) {
      fullCommand.addAll(prefixCommands);
    }

    fullCommand.add(containerExecutorExe);

    String cliSwitch = operation.getOperationType().getOption();

    if (!cliSwitch.isEmpty()) {
      fullCommand.add(cliSwitch);
    }

    fullCommand.addAll(operation.getArguments());

    String[] fullCommandArray =
        fullCommand.toArray(new String[fullCommand.size()]);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Privileged Execution Command Array: " +
          Arrays.toString(fullCommandArray));
    }

    return fullCommandArray;
  }

  /**
   * 执行特权操作，通过container-executor二进制文件完成需要root权限的系统操作
   * @param prefixCommands 前缀命令（如nice调整优先级）
   * @param operation 待执行的特权操作
   * @param workingDir 执行工作目录（可选）
   * @param env 执行环境变量（可选）
   * @param grabOutput 是否捕获命令标准输出
   * @param inheritParentEnv 是否继承父进程环境变量
   * @return 若grabOutput为true，返回命令标准输出，否则返回null
   * @throws PrivilegedOperationException 执行失败时抛出
   */
  public String executePrivilegedOperation(List<String> prefixCommands,
      PrivilegedOperation operation, File workingDir,
      Map<String, String> env, boolean grabOutput, boolean inheritParentEnv)
      throws PrivilegedOperationException {
    // 构造完整执行命令
    String[] fullCommandArray = getPrivilegedOperationExecutionCommand
        (prefixCommands, operation);
    // 创建shell命令执行器
    ShellCommandExecutor exec = new ShellCommandExecutor(fullCommandArray,
        workingDir, env, 0L, inheritParentEnv);

    try {
      // 执行命令
      exec.execute();
      if (LOG.isDebugEnabled()) {
        LOG.debug("command array:");
        LOG.debug(Arrays.toString(fullCommandArray));
        LOG.debug("Privileged Execution Operation Output:");
        LOG.debug(exec.getOutput());
      }
    } catch (ExitCodeException e) {
      // 非零退出码，处理异常
      if (operation.isFailureLoggingEnabled()) {
        // 构建错误日志，包含退出码、标准错误、标准输出和完整命令
        StringBuilder logBuilder = new StringBuilder("Shell execution returned "
            + "exit code: ")
            .append(exec.getExitCode())
            .append(". Privileged Execution Operation Stderr: ")
            .append(System.lineSeparator())
            .append(e.getMessage())
            .append(System.lineSeparator())
            .append("Stdout: " + exec.getOutput())
            .append(System.lineSeparator());
        logBuilder.append("Full command array for failed execution: ")
            .append(System.lineSeparator());
        logBuilder.append(Arrays.toString(fullCommandArray));

        LOG.warn(logBuilder.toString());
      }

      // 包装异常，保留输出信息
      throw new PrivilegedOperationException(e, e.getExitCode(),
          exec.getOutput(), e.getMessage());
    } catch (IOException e) {
      // IO异常处理
      LOG.warn("IOException executing command: ", e);
      throw new PrivilegedOperationException(e);
    }

    // 返回捕获的输出
    if (grabOutput) {
      return exec.getOutput();
    }

    return null;
  }

  /**
   * 简化版特权操作执行，使用默认参数
   * @param operation 待执行的特权操作
   * @param grabOutput 是否捕获命令标准输出
   * @return 若grabOutput为true，返回命令标准输出，否则返回null
   * @throws PrivilegedOperationException 执行失败时抛出
   */
  public String executePrivilegedOperation(PrivilegedOperation operation,
      boolean grabOutput) throws PrivilegedOperationException {
    return executePrivilegedOperation(null, operation, null, null, grabOutput,
        false);
  }

  /**
   * 执行交互式特权操作，启动子进程并返回stdin/stdout流对供交互
   * @param prefixCommands 前缀命令（如nice调整优先级）
   * @param operation 待执行的特权操作
   * @return 包含stdout和stdin的流对，可用于和子进程交互
   * @throws PrivilegedOperationException 执行失败时抛出
   * @throws InterruptedException 线程中断时抛出
   */
  public IOStreamPair executePrivilegedInteractiveOperation(
      List<String> prefixCommands, PrivilegedOperation operation)
      throws PrivilegedOperationException, InterruptedException {
    // 构造完整执行命令
    String[] fullCommandArray = getPrivilegedOperationExecutionCommand(
        prefixCommands, operation);
    // 使用ProcessBuilder启动子进程
    ProcessBuilder pb = new ProcessBuilder(fullCommandArray);
    OutputStream stdin;
    InputStream stdout;
    try {
      // 将错误流合并到标准输出
      pb.redirectErrorStream(true);
      Process p = pb.start();
      // 获取子进程的输入输出流
      stdin = p.getOutputStream();
      stdout = p.getInputStream();

      if (LOG.isDebugEnabled()) {
        LOG.debug("command array:");
        LOG.debug(Arrays.toString(fullCommandArray));
      }
    } catch (ExitCodeException e) {
      // 非零退出码异常处理
      if (operation.isFailureLoggingEnabled()) {
        StringBuilder logBuilder = new StringBuilder(
            "Interactive Shell execution returned exit code: ")
            .append(e.getExitCode())
            .append(". Privileged Interactive Operation Stderr: ")
            .append(System.lineSeparator())
            .append(e.getMessage())
            .append(System.lineSeparator());
        logBuilder.append("Full command array for failed execution: ")
            .append(System.lineSeparator());
        logBuilder.append(Arrays.toString(fullCommandArray));

        LOG.warn(logBuilder.toString());
      }

      throw new PrivilegedOperationException(e, e.getExitCode(),
          pb.redirectError().toString(), e.getMessage());
    } catch (IOException e) {
      // IO异常处理
      LOG.warn("IOException executing command: ", e);
      throw new PrivilegedOperationException(e);
    }

    // 返回流对：第一个元素是stdout，第二个是stdin
    return new IOStreamPair(stdout, stdin);
  }

  // 特权操作合并工具函数，将多个小操作合并为一个减少进程启动开销
  // 未来计划支持通用合并规则，支持不同类型操作的合并

  /**
   * 合并多个添加PID到cgroup的特权操作为单个操作，减少进程启动开销
   * @param ops 待合并的cgroup操作列表，当前仅支持ADD_PID_TO_CGROUP类型
   * @return 合并后的单个特权操作，输入为空时返回null
   * @throws PrivilegedOperationException 操作类型不合法或参数错误时抛出
   */
  public static PrivilegedOperation squashCGroupOperations
  (List<PrivilegedOperation> ops) throws PrivilegedOperationException {
    if (ops.size() == 0) {
      return null;
    }

    // 构建最终操作参数字符串，前缀为cgroup参数标记
    StringBuilder finalOpArg = new StringBuilder(PrivilegedOperation
        .CGROUP_ARG_PREFIX);
    boolean noTasks = true;

    // 遍历所有待合并操作，提取tasks文件路径
    for (PrivilegedOperation op : ops) {
      // 检查操作类型，仅支持ADD_PID_TO_CGROUP
      if (!op.getOperationType()
          .equals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP)) {
        LOG.warn("Unsupported operation type: " + op.getOperationType());
        throw new PrivilegedOperationException("Unsupported operation type:"
            + op.getOperationType());
      }

      // 检查参数数量，每个操作必须只有一个参数
      List<String> args = op.getArguments();
      if (args.size() != 1) {
        LOG.warn("Invalid number of args: " + args.size());
        throw new PrivilegedOperationException("Invalid number of args: "
            + args.size());
      }

      String arg = args.get(0);
      // 提取前缀后的tasks文件路径
      String tasksFile = StringUtils.substringAfter(arg,
          PrivilegedOperation.CGROUP_ARG_PREFIX);
      if (tasksFile == null || tasksFile.isEmpty()) {
        LOG.warn("Invalid argument: " + arg);
        throw new PrivilegedOperationException("Invalid argument: " + arg);
      }

      // 跳过空tasks标记
      if (tasksFile.equals(PrivilegedOperation.CGROUP_ARG_NO_TASKS)) {
        continue;
      }

      // 多个tasks文件之间使用路径分隔符拼接
      if (noTasks == false) {
        finalOpArg.append(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR);
        finalOpArg.append(tasksFile);
      } else {
        finalOpArg.append(tasksFile);
        noTasks = false;
      }
    }

    // 没有有效tasks文件，添加空标记
    if (noTasks) {
      finalOpArg.append(PrivilegedOperation.CGROUP_ARG_NO_TASKS);
    }

    // 创建合并后的操作并返回
    PrivilegedOperation finalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP, finalOpArg
        .toString());

    return finalOp;
  }
}