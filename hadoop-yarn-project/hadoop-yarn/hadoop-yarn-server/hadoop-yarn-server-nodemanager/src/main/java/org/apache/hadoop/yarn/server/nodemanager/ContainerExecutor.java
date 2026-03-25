// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.ShellScriptBuilder;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerPrepareContext;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.CONTAINER_PRE_LAUNCH_STDOUT;

/**
 * 文件概述：容器执行器抽象基类，定义了YARN NodeManager上启动和管理容器的核心接口与通用实现
 * 核心职责：抽象不同操作系统/容器 runtime 的容器生命周期管理接口，提供通用的环境准备、脚本生成逻辑
 * This class is abstraction of the mechanism used to launch a container on the
 * underlying OS.  All executor implementations must extend ContainerExecutor.
 */
public abstract class ContainerExecutor implements Configurable {
  private static final Logger LOG =
       LoggerFactory.getLogger(ContainerExecutor.class);
  protected static final String WILDCARD = "*";
  public static final String TOKEN_FILE_NAME_FMT = "%s.tokens";

  /**
   * The permissions to use when creating the launch script.
   */
  public static final FsPermission TASK_LAUNCH_SCRIPT_PERMISSION =
      FsPermission.createImmutable((short)0700);

  /**
   * The relative path to which debug information will be written.
   *
   * @see ShellScriptBuilder#listDebugInformation
   */
  public static final String DIRECTORY_CONTENTS = "directory.info";

  private Configuration conf;
  // 存储容器ID对应PID文件路径的映射，用于跟踪活跃容器
  private final ConcurrentMap<ContainerId, Path> pidFiles =
      new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  // 环境变量白名单，允许从NodeManager继承到容器的环境变量列表
  private String[] whitelistVars;
  // 等待容器退出码文件生成的超时时间
  private int exitCodeFileTimeout =
      YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_EXIT_FILE_TIMEOUT;
  private int containerExitCode;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (conf != null) {
      // 从配置加载环境变量白名单，按逗号分割
      whitelistVars = conf.get(YarnConfiguration.NM_ENV_WHITELIST,
          YarnConfiguration.DEFAULT_NM_ENV_WHITELIST).split(",");
      // 从配置加载退出码文件超时时间
      exitCodeFileTimeout = conf.getInt(
          YarnConfiguration.NM_CONTAINER_EXECUTOR_EXIT_FILE_TIMEOUT,
          YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_EXIT_FILE_TIMEOUT);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * 执行容器执行器初始化，验证配置和权限是否正确
   * Run the executor initialization steps.
   * Verify that the necessary configs and permissions are in place.
   *
   * @param nmContext NodeManager上下文
   * @throws IOException if initialization fails
   */
  public abstract void init(Context nmContext) throws IOException;

  public void start() {}

  public void stop() {}

  /**
   * This function localizes the JAR file on-demand.
   * On Windows the ContainerLaunch creates a temporary special JAR manifest of
   * other JARs to workaround the CLASSPATH length. In a secure cluster this
   * JAR must be localized so that the container has access to it.
   * The default implementation returns the classpath passed to it, which
   * is expected to have been created in the node manager's <i>fprivate</i>
   * folder, which will not work with secure Windows clusters.
   *
   * @param jarPath the path to the JAR to localize
   * @param target the directory where the JAR file should be localized
   * @param owner the name of the user who should own the localized file
   * @return the path to the localized JAR file
   * @throws IOException if localization fails
   */
  public Path localizeClasspathJar(Path jarPath, Path target, String owner)
      throws IOException {
    return jarPath;
  }


  /**
   * Prepare the environment for containers in this application to execute.
   * <pre>
   * For $x in local.dirs
   *   create $x/$user/$appId
   * Copy $nmLocal/appTokens {@literal ->} $N/$user/$appId
   * For $rsrc in private resources
   *   Copy $rsrc {@literal ->} $N/$user/filecache/[idef]
   * For $rsrc in job resources
   *   Copy $rsrc {@literal ->} $N/$user/$appId/filecache/idef
   * </pre>
   *
   * @param ctx LocalizerStartContext that encapsulates necessary information
   *            for starting a localizer.
   * @throws IOException for most application init failures
   * @throws InterruptedException if application init thread is halted by NM
   * @throws ConfigurationException if config error was found
   */
  public abstract void startLocalizer(LocalizerStartContext ctx)
      throws IOException, InterruptedException, ConfigurationException;

  /**
   * 在写入启动环境前准备容器
   * Prepare the container prior to the launch environment being written.
   * @param ctx Encapsulates information necessary for launching containers.
   * @throws IOException if errors occur during container preparation
   */
  public void prepareContainer(ContainerPrepareContext ctx) throws
      IOException{
  }

  /**
   * 在当前节点启动容器，阻塞调用直到容器退出
   * Launch the container on the node. This is a blocking call and returns only
   * when the container exits.
   * @param ctx Encapsulates information necessary for launching containers.
   * @return the return status of the launch
   * @throws IOException if the container launch fails
   * @throws ConfigurationException if config error was found
   */
  public abstract int launchContainer(ContainerStartContext ctx) throws
      IOException, ConfigurationException;

  /**
   * 在当前节点重新启动容器，阻塞调用直到容器退出
   * Relaunch the container on the node. This is a blocking call and returns
   * only when the container exits.
   * @param ctx Encapsulates information necessary for relaunching containers.
   * @return the return status of the relaunch
   * @throws IOException if the container relaunch fails
   * @throws ConfigurationException if config error was found
   */
  public abstract int relaunchContainer(ContainerStartContext ctx) throws
      IOException, ConfigurationException;

  /**
   * 向容器发送指定信号
   * Signal container with the specified signal.
   *
   * @param ctx Encapsulates information necessary for signaling containers.
   * @return returns true if the operation succeeded
   * @throws IOException if signaling the container fails
   */
  public abstract boolean signalContainer(ContainerSignalContext ctx)
      throws IOException;

  /**
   * 执行容器善后清理工作
   * Perform the steps necessary to reap the container.
   *
   * @param ctx Encapsulates information necessary for reaping containers.
   * @return returns true if the operation succeeded.
   * @throws IOException if reaping the container fails.
   */
  public abstract boolean reapContainer(ContainerReapContext ctx)
      throws IOException;

  /**
   * 在运行容器中执行交互式命令
   * Perform interactive docker command into running container.
   *
   * @param ctx Encapsulates information necessary for exec containers.
   * @return return input/output stream if the operation succeeded.
   * @throws ContainerExecutionException if container exec fails.
   */
  public abstract IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException;

  /**
   * 以指定用户身份删除指定目录
   * Delete specified directories as a given user.
   *
   * @param ctx Encapsulates information necessary for deletion.
   * @throws IOException if delete fails
   * @throws InterruptedException if interrupted while waiting for the deletion
   * operation to complete
   */
  public abstract void deleteAsUser(DeletionAsUserContext ctx)
      throws IOException, InterruptedException;

  /**
   * 创建指向目标的符号链接
   * Create a symlink file which points to the target.
   * @param target The target for symlink
   * @param symlink the symlink file
   * @throws IOException Error when creating symlinks
   */
  public abstract void symLink(String target, String symlink)
      throws IOException;

  /**
   * 检查容器是否存活
   * Check if a container is alive.
   * @param ctx Encapsulates information necessary for container liveness check.
   * @return true if container is still alive
   * @throws IOException if there is a failure while checking the container
   * status
   */
  public abstract boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException;


  public Map<String, LocalResource> getLocalResources(Container container)
      throws IOException {
    return container.getLaunchContext().getLocalResources();
  }

  /**
   * 更新容器内的集群信息
   * Update cluster information inside container.
   *
   * @param ctx ContainerRuntimeContext
   * @param user Owner of application
   * @param appId YARN application ID
   * @param spec Service Specification
   * @throws IOException if there is a failure while writing spec to disk
   */
  public abstract void updateYarnSysFS(Context ctx, String user,
      String appId, String spec) throws IOException;

  /**
   * 重新获取已存在容器的控制权，阻塞直到容器退出，用于NodeManager重启恢复
   * Recover an already existing container. This is a blocking call and returns
   * only when the container exits.  Note that the container must have been
   * activated prior to this call.
   *
   * @param ctx encapsulates information necessary to reacquire container
   * @return The exit code of the pre-existing container
   * @throws IOException if there is a failure while reacquiring the container
   * @throws InterruptedException if interrupted while waiting to reacquire
   * the container
   */
  public int reacquireContainer(ContainerReacquisitionContext ctx)
      throws IOException, InterruptedException {
    Container container = ctx.getContainer();
    String user = ctx.getUser();
    ContainerId containerId = ctx.getContainerId();
    Path pidPath = getPidFilePath(containerId);

    if (pidPath == null) {
      LOG.warn("{} is not active, returning terminated error", containerId);
      containerExitCode = ExitCode.TERMINATED.getExitCode();
      return ExitCode.TERMINATED.getExitCode();
    }

    String pid = ProcessIdFileReader.getProcessId(pidPath);

    if (pid == null) {
      throw new IOException("Unable to determine pid for " + containerId);
    }

    LOG.info("Reacquiring {} with pid {}", containerId, pid);

    ContainerLivenessContext livenessContext = new ContainerLivenessContext
        .Builder()
        .setContainer(container)
        .setUser(user)
        .setPid(pid)
        .build();

    // 循环等待容器退出
    while (isContainerAlive(livenessContext)) {
      Thread.sleep(1000);
    }

    // wait for exit code file to appear
    final int sleepMsec = 100;
    int msecLeft = this.exitCodeFileTimeout;
    String exitCodeFile = ContainerLaunch.getExitCodeFile(pidPath.toString());
    File file = new File(exitCodeFile);

    // 等待退出码文件生成，超时则报错
    while (!file.exists() && msecLeft >= 0) {
      if (!isContainerActive(containerId)) {
        LOG.info("{} was deactivated", containerId);
        containerExitCode = ExitCode.TERMINATED.getExitCode();
        return ExitCode.TERMINATED.getExitCode();
      }

      Thread.sleep(sleepMsec);

      msecLeft -= sleepMsec;
    }

    if (msecLeft < 0) {
      throw new IOException("Timeout while waiting for exit code from "
          + containerId);
    }

    try {
      // 读取并解析退出码
      containerExitCode = Integer.parseInt(
          FileUtils.readFileToString(file, StandardCharsets.UTF_8).trim());
      return containerExitCode;
    } catch (NumberFormatException e) {
      throw new IOException("Error parsing exit code from pid " + pid, e);
    }
  }

  /**
   * 将容器启动环境写入默认容器启动脚本
   * This method writes out the launch environment of a container to the
   * default container launch script. For the default container script path see
   * {@link ContainerLaunch#CONTAINER_SCRIPT}.
   *
   * @param out the output stream to which the environment is written (usually
   * a script file which will be executed by the Launcher)
   * @param environment the environment variables and their values
   * @param resources the resources which have been localized for this
   * container. Symlinks will be created to these localized resources
   * @param command the command that will be run
   * @param logDir the log dir to which to copy debugging information
   * @param user the username of the job owner
   * @param nmVars the set of environment vars that are explicitly set by NM
   * @throws IOException if any errors happened writing to the OutputStream,
   * while creating symlinks
   */
  public void writeLaunchEnv(OutputStream out, Map<String, String> environment,
      Map<Path, List<String>> resources, List<String> command, Path logDir,
      String user, LinkedHashSet<String> nmVars) throws IOException {
    this.writeLaunchEnv(out, environment, resources, command, logDir, user,
        ContainerLaunch.CONTAINER_SCRIPT, nmVars);
  }

  /**
   * This method writes out the launch environment of a container to a specified
   * path.
   *
   * @param out the output stream to which the environment is written (usually
   * a script file which will be executed by the Launcher)
   * @param environment the environment variables and their values