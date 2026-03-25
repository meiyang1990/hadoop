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

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.numaAwarenessEnabled;

import org.apache.hadoop.classification.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa.NumaResourceAllocation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa.NumaResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default容器执行器实现，提供通用容器执行服务。
 * 通过{@link ProcessBuilder}以平台独立方式处理进程执行，负责在NodeManager节点上启动、管理容器生命周期。
 */
public class DefaultContainerExecutor extends ContainerExecutor {

  private static final Logger LOG =
       LoggerFactory.getLogger(DefaultContainerExecutor.class);

  private static final int WIN_MAX_PATH = 260;

  /**
   * 本地文件系统的FileContext实例，用于操作本地文件。
   */
  protected final FileContext lfs;

  private String logDirPermissions = null;

  private NumaResourceAllocator numaResourceAllocator;


  private String numactl;
  /**
   * 默认构造函数，供测试使用。
   */
  @VisibleForTesting
  public DefaultContainerExecutor() {
    try {
      this.lfs = FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 带指定FileContext的构造函数。
   *
   * @param lfs 文件系统上下文
   */
  DefaultContainerExecutor(FileContext lfs) {
    this.lfs = lfs;
  }

  /**
   * 使用本地文件上下文复制文件。
   *
   * @param src 源文件路径
   * @param dst 目标文件路径
   * @param owner 新文件所有者，仅安全Windows集群使用
   * @throws IOException 复制失败时抛出
   */
  protected void copyFile(Path src, Path dst, String owner) throws IOException {
    lfs.util().copy(src, dst, false, true);
  }
  
  /**
   * 设置脚本文件可执行权限。
   *
   * @param script 脚本路径
   * @param owner 文件新所有者，仅安全Windows集群使用
   * @throws IOException 修改权限失败时抛出
   */
  protected void setScriptExecutable(Path script, String owner)
      throws IOException {
    lfs.setPermission(script, ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
  }

  @Override
  public void init(Context nmContext) throws IOException {
    // 如果启用了NUMA感知，则初始化NUMA资源分配器
    if(numaAwarenessEnabled(getConf())) {
      numaResourceAllocator = new NumaResourceAllocator(nmContext);
      numactl = this.getConf().get(YarnConfiguration.NM_NUMA_AWARENESS_NUMACTL_CMD,
              YarnConfiguration.DEFAULT_NM_NUMA_AWARENESS_NUMACTL_CMD);
      try {
        numaResourceAllocator.init(this.getConf());
        LOG.info("NUMA resources allocation is enabled in DefaultContainer Executor," +
                " Successfully initialized NUMA resources allocator.");
      } catch (YarnException e) {
        LOG.warn("Improper NUMA configuration provided.", e);
        throw new IOException("Failed to initialize configured numa subsystem!");
      }
    }
  }

  @Override
  public void startLocalizer(LocalizerStartContext ctx)
      throws IOException, InterruptedException {
    Path nmPrivateContainerTokensPath = ctx.getNmPrivateContainerTokens();
    InetSocketAddress nmAddr = ctx.getNmAddr();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String locId = ctx.getLocId();
    LocalDirsHandlerService dirsHandler = ctx.getDirsHandler();

    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();
    
    // 创建各级用户目录结构
    createUserLocalDirs(localDirs, user);
    createUserCacheDirs(localDirs, user);
    createAppDirs(localDirs, user, appId);
    createAppLogDirs(appId, logDirs, user);

    // 按可用空间权重随机选择一个本地目录作为应用工作目录
    Path appStorageDir = getWorkingDir(localDirs, user, appId);

    String tokenFn = String.format(TOKEN_FILE_NAME_FMT, locId);
    Path tokenDst = new Path(appStorageDir, tokenFn);
    // 复制容器令牌文件到工作目录
    copyFile(nmPrivateContainerTokensPath, tokenDst, user);
    LOG.info("Copying from {} to {}", nmPrivateContainerTokensPath, tokenDst);


    FileContext localizerFc =
        FileContext.getFileContext(lfs.getDefaultFileSystem(), getConf());
    localizerFc.setUMask(lfs.getUMask());
    // 设置本地化器工作目录
    localizerFc.setWorkingDirectory(appStorageDir);
    LOG.info("Localizer CWD set to {} = {}", appStorageDir,
        localizerFc.getWorkingDirectory());

    ContainerLocalizer localizer =
        createContainerLocalizer(user, appId, locId, tokenFn, localDirs,
            localizerFc);
    // TODO: DO it over RPC for maintaining similarity?
    // 执行容器资源本地化
    localizer.runLocalization(nmAddr);
  }

  /**
   * 创建ContainerLocalizer实例。
   *
   * @param user 应用对应用户
   * @param appId 应用ID
   * @param locId 容器本地化ID
   * @param tokenFileName 令牌文件名
   * @param localDirs 本地目录列表
   * @param localizerFc 本地化使用的文件上下文
   * @return 新的ContainerLocalizer实例
   * @throws IOException 初始化失败时抛出
   */
  @Private
  @VisibleForTesting
  protected ContainerLocalizer createContainerLocalizer(String user,
      String appId, String locId, String tokenFileName, List<String> localDirs,
      FileContext localizerFc) throws IOException {
    ContainerLocalizer localizer =
        new ContainerLocalizer(localizerFc, user, appId, locId, tokenFileName,
            getPaths(localDirs),
            RecordFactoryProvider.getRecordFactory(getConf()));
    return localizer;
  }

  @Override
  public int launchContainer(ContainerStartContext ctx)
      throws IOException, ConfigurationException {
    Container container = ctx.getContainer();
    Path nmPrivateContainerScriptPath = ctx.getNmPrivateContainerScriptPath();
    Path nmPrivateTokensPath = ctx.getNmPrivateTokensPath();
    Path nmPrivateKeystorePath = ctx.getNmPrivateKeystorePath();
    Path nmPrivateTruststorePath = ctx.getNmPrivateTruststorePath();
    String user = ctx.getUser();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();

    FsPermission dirPerm = new FsPermission(APPDIR_PERM);
    ContainerId containerId = container.getContainerId();

    // 在所有本地磁盘上创建容器目录
    String containerIdStr = containerId.toString();
    String appIdStr =
            containerId.getApplicationAttemptId().
                getApplicationId().toString();
    for (String sLocalDir : localDirs) {
      Path usersdir = new Path(sLocalDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, user);
      Path appCacheDir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(appCacheDir, appIdStr);
      Path containerDir = new Path(appDir, containerIdStr);
      createDir(containerDir, dirPerm, true, user);
    }

    // 在所有日志磁盘上创建容器日志目录
    createContainerLogDirs(appIdStr, containerIdStr, logDirs, user);

    // 创建容器临时目录
    Path tmpDir = new Path(containerWorkDir,
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    createDir(tmpDir, dirPerm, false, user);


    // 复制容器令牌到工作目录
    Path tokenDst =
      new Path(containerWorkDir, ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE);
    copyFile(nmPrivateTokensPath, tokenDst, user);

    // 复制keystore到工作目录（如果存在）
    if (nmPrivateKeystorePath != null) {
      Path keystoreDst =
          new Path(containerWorkDir, ContainerLaunch.KEYSTORE_FILE);
      copyFile(nmPrivateKeystorePath, keystoreDst, user);
    }

    // 复制truststore到工作目录（如果存在）
    if (nmPrivateTruststorePath != null) {
      Path truststoreDst =
          new Path(containerWorkDir, ContainerLaunch.TRUSTSTORE_FILE);
      copyFile(nmPrivateTruststorePath, truststoreDst, user);
    }

    // 复制启动脚本到工作目录
    Path launchDst =
        new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
    copyFile(nmPrivateContainerScriptPath, launchDst, user);

    // 创建本地启动包装脚本
    LocalWrapperScriptBuilder sb = getLocalWrapperScriptBuilder(
        containerIdStr, containerWorkDir); 

    // Windows下提前检查路径长度，避免启动失败
    if (Shell.WINDOWS &&
        sb.getWrapperScriptPath().toString().length() > WIN_MAX_PATH) {
      throw new IOException(String.format(
        "Cannot launch container using script at path %s, because it exceeds " +
        "the maximum supported path length of %d characters.  Consider " +
        "configuring shorter directories in %s.", sb.getWrapperScriptPath(),
        WIN_MAX_PATH, YarnConfiguration.NM_LOCAL_DIRS));
    }

    // 获取PID文件路径
    Path pidFile = getPidFilePath(containerId);
    if (pidFile != null) {
      // 写入包装脚本内容
      sb.writeLocalWrapperScript(launchDst, pidFile);
    } else {
      LOG.info("Container {} pid file not set. Returning terminated error",
          containerIdStr);
      return ExitCode.TERMINATED.getExitCode();
    }
    
    // 准备执行启动脚本
    Shell.CommandExecutor shExec = null;
    try {
      // 设置脚本可执行权限
      setScriptExecutable(launchDst, user);
      setScriptExecutable(sb.getWrapperScriptPath(), user);

      // 根据配置添加NUMA相关命令前缀
      String[] numaCommands = new String[]{};

      // 如果启用了NUMA分配，获取对应NUMA命令参数
      if (numaResourceAllocator != null) {
        try {
          NumaResourceAllocation numaResourceAllocation =
                  numaResourceAllocator.allocateNumaNodes(container);
          if (numaResourceAllocation != null) {
            numaCommands = getNumaCommands(numaResourceAllocation);
          }
        } catch (ResourceHandlerException e) {
          LOG.error("NumaResource Allocation failed!", e);
          throw new IOException("NumaResource Allocation Error!", e);
        }
      }

      // 构建命令执行器
      shExec = buildCommandExecutor(sb.getWrapperScriptPath().toString(),
              containerIdStr, user, pidFile, container.getResource(),
              new File(containerWorkDir.toUri().getPath()),
              container.getLaunchContext().getEnvironment(),
              numaCommands);

      // 容器仍处于活跃状态则执行启动
      if (isContainerActive(containerId)) {
        shExec.execute();
      } else {
        LOG.info("Container {} was marked as inactive. "
            + "Returning terminated error", containerIdStr);
        return ExitCode.TERMINATED.getExitCode();
      }
    } catch (IOException e) {
      if (null == shExec) {
        return -1;
      }
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container {} is : {}", containerId, exitCode);
      // 143(SIGTERM)和137(SIGKILL)表示容器被强制杀死，其他情况记录诊断信息
      if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
          && exitCode != ExitCode.TERMINATED.getExitCode()) {
        LOG.warn("Exception from container-launch with container ID: {}"
            + " and exit code: {}", containerId, exitCode, e);

        // 构建诊断信息
        StringBuilder builder = new StringBuilder();
        builder.append("Exception from container-launch.\n")
            .append("Container id: ").append(containerId).append("\n")
            .append("Exit code: ").append(exitCode).append("\n");
        if (!Optional.ofNullable(e.getMessage()).orElse("").isEmpty()) {
          builder.append("Exception message: ")
              .append(e.getMessage()).append("\n");
        }

        if (!shExec.getOutput().isEmpty()) {
          builder.append("Shell output: ")
              .append(shExec.getOutput()).append("\n");
        }
        String diagnostics = builder.toString();
        // 记录输出并更新容器诊断信息
        logOutput(diagnostics);
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            diagnostics));
      } else {
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            "Container killed on request. Exit code is " + exitCode));
      }
      return exitCode;
    } finally {
      if (shExec != null) shExec.close();
      // 容器完成后执行清理
      postComplete(containerId);
    }
    return 0;
  }

  @Override
  public int relaunchContainer(ContainerStartContext ctx)
      throws IOException, ConfigurationException {
    return launchContainer(ctx);
  }

  /**
   * 基于参数构建Shell命令执行器。
   *
   * @param wrapperScriptPath 包装脚本路径
   * @param containerIdStr 容器ID字符串
   * @param user 应用用户名
   * @param pidFile PID