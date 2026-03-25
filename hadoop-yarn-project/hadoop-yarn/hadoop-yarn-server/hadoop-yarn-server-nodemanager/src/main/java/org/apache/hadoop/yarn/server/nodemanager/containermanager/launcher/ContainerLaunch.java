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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.TOKEN_FILE_NAME_FMT;

import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerPrepareContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.yarn.server.security.AMSecretKeys;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN NodeManager上容器启动任务，负责处理容器从环境准备、脚本生成到实际启动、退出处理全流程
 * 实现Callable接口，异步执行容器启动逻辑
 */
public class ContainerLaunch implements Callable<Integer> {

  private static final Logger LOG =
       LoggerFactory.getLogger(ContainerLaunch.class);

  private static final String CONTAINER_PRE_LAUNCH_PREFIX = "prelaunch";
  public static final String CONTAINER_PRE_LAUNCH_STDOUT = CONTAINER_PRE_LAUNCH_PREFIX + ".out";
  public static final String CONTAINER_PRE_LAUNCH_STDERR = CONTAINER_PRE_LAUNCH_PREFIX + ".err";

  public static final String CONTAINER_SCRIPT =
    Shell.appendScriptExtension("launch_container");

  public static final String FINAL_CONTAINER_TOKENS_FILE = "container_tokens";
  public static final String SYSFS_DIR = "sysfs";

  public static final String KEYSTORE_FILE = "yarn_provided.keystore";
  public static final String TRUSTSTORE_FILE = "yarn_provided.truststore";

  private static final String PID_FILE_NAME_FMT = "%s.pid";
  static final String EXIT_CODE_FILE_SUFFIX = ".exitcode";

  // JDK17+额外需要打开的模块参数
  private static final String ADDITIONAL_JDK17_PLUS_OPTIONS =
      "--add-opens=java.base/java.lang=ALL-UNNAMED " +
      "--add-exports=java.base/sun.net.dns=ALL-UNNAMED " +
      "--add-exports=java.base/sun.net.util=ALL-UNNAMED";

  protected final Dispatcher dispatcher;
  protected final ContainerExecutor exec;
  protected final Application app;
  protected final Container container;
  private final Configuration conf;
  private final Context context;
  private final ContainerManagerImpl containerManager;

  // 标记容器是否已经启动，避免重复启动
  protected AtomicBoolean containerAlreadyLaunched = new AtomicBoolean(false);
  // 标记是否需要暂停容器
  protected AtomicBoolean shouldPauseContainer = new AtomicBoolean(false);

  // 标记容器启动流程是否已完成
  protected AtomicBoolean completed = new AtomicBoolean(false);

  private volatile boolean killedBeforeStart = false;
  private long maxKillWaitTime = 2000;

  protected Path pidFilePath = null;

  protected final LocalDirsHandlerService dirsHandler;

  // 启动锁，保护容器启动/清理并发操作
  private final Lock launchLock = new ReentrantLock();

  /**
   * 构造容器启动任务
   * @param context NodeManager上下文
   * @param configuration 配置对象
   * @param dispatcher 事件分发器
   * @param exec 容器执行器
   * @param app 所属应用
   * @param container 待启动容器
   * @param dirsHandler 本地目录处理器
   * @param containerManager 容器管理器
   */
  public ContainerLaunch(Context context, Configuration configuration,
      Dispatcher dispatcher, ContainerExecutor exec, Application app,
      Container container, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager) {
    this.context = context;
    this.conf = configuration;
    this.app = app;
    this.exec = exec;
    this.container = container;
    this.dispatcher = dispatcher;
    this.dirsHandler = dirsHandler;
    this.containerManager = containerManager;
    this.maxKillWaitTime =
        conf.getLong(YarnConfiguration.NM_PROCESS_KILL_WAIT_MS,
            YarnConfiguration.DEFAULT_NM_PROCESS_KILL_WAIT_MS);
  }

  /**
   * 展开环境变量中的占位符，替换特定变量和平台相关格式
   * @param var 原始变量字符串
   * @param containerLogDir 容器日志目录
   * @return 展开后的字符串
   */
  @VisibleForTesting
  public static String expandEnvironment(String var,
      Path containerLogDir) {
    var = var.replace(ApplicationConstants.LOG_DIR_EXPANSION_VAR,
      containerLogDir.toString());
    var = var.replace(ApplicationConstants.CLASS_PATH_SEPARATOR,
      File.pathSeparator);

    if (Shell.isJavaVersionAtLeast(17)) {
      var = var.replace(ApplicationConstants.JVM_ADD_OPENS_VAR, ADDITIONAL_JDK17_PLUS_OPTIONS);
    } else {
      var = var.replace(ApplicationConstants.JVM_ADD_OPENS_VAR, "");
    }

    // 替换参数展开标记，Windows使用%VAR%，Linux使用$VAR
    if (Shell.WINDOWS) {
      var = var.replaceAll("(\\{\\{)|(\\}\\})", "%");
    } else {
      var = var.replace(ApplicationConstants.PARAMETER_EXPANSION_LEFT, "$");
      var = var.replace(ApplicationConstants.PARAMETER_EXPANSION_RIGHT, "");
    }
    return var;
  }

  /**
   * 展开环境变量Map中所有值的占位符
   */
  private void expandAllEnvironmentVars(
      Map<String, String> environment, Path containerLogDir) {
    for (Entry<String, String> entry : environment.entrySet()) {
      String value = entry.getValue();
      value = expandEnvironment(value, containerLogDir);
      entry.setValue(value);
    }
  }

  private void addKeystoreVars(Map<String, String> environment,
      Path containerWorkDir) {
    environment.put(ApplicationConstants.KEYSTORE_FILE_LOCATION_ENV_NAME,
        new Path(containerWorkDir,
            ContainerLaunch.KEYSTORE_FILE).toUri().getPath());
    environment.put(ApplicationConstants.KEYSTORE_PASSWORD_ENV_NAME,
        new String(container.getCredentials().getSecretKey(
            AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE_PASSWORD),
            StandardCharsets.UTF_8));
  }

  private void addTruststoreVars(Map<String, String> environment,
                               Path containerWorkDir) {
    environment.put(
        ApplicationConstants.TRUSTSTORE_FILE_LOCATION_ENV_NAME,
        new Path(containerWorkDir,
            ContainerLaunch.TRUSTSTORE_FILE).toUri().getPath());
    environment.put(ApplicationConstants.TRUSTSTORE_PASSWORD_ENV_NAME,
        new String(container.getCredentials().getSecretKey(
            AMSecretKeys.YARN_APPLICATION_AM_TRUSTSTORE_PASSWORD),
            StandardCharsets.UTF_8));
  }

  @Override
  public Integer call() {
    // 检查容器状态，若已被杀死则直接返回
    if (!validateContainerState()) {
      return 0;
    }

    final ContainerLaunchContext launchContext = container.getLaunchContext();
    ContainerId containerID = container.getContainerId();
    String containerIdStr = containerID.toString();
    final List<String> command = launchContext.getCommands();
    int ret = -1;

    Path containerLogDir;
    try {
      // 获取已本地化的资源列表
      Map<Path, List<String>> localResources = getLocalizedResources();

      final String user = container.getUser();
      // /////////////////////////// 变量展开
      // 在写出容器脚本之前完成变量展开
      List<String> newCmds = new ArrayList<String>(command.size());
      String appIdStr = app.getAppId().toString();
      String relativeContainerLogDir = ContainerLaunch
          .getRelativeContainerLogDir(appIdStr, containerIdStr);
      // 获取容器日志写入目录
      containerLogDir =
          dirsHandler.getLogPathForWrite(relativeContainerLogDir, false);
      // 记录容器日志目录到NM状态存储
      recordContainerLogDir(containerID, containerLogDir.toString());
      // 展开每个命令中的占位符
      for (String str : command) {
        newCmds.add(expandEnvironment(str, containerLogDir));
      }
      // 设置展开后的命令
      launchContext.setCommands(newCmds);

      // 环境变量展开在addConfigsToEnv之后执行，允许NM管理员配置的环境变量引用用户定义的变量
      Map<String, String> environment = launchContext.getEnvironment();
      // /////////////////////////// 变量扩张结束

      // 记录NM添加的环境变量，用于后续排序处理
      LinkedHashSet<String> nmEnvVars = new LinkedHashSet<String>();

      // 获取本地文件系统上下文
      FileContext lfs = FileContext.getLocalFSFileContext();

      // 获取NM私有目录下容器脚本路径
      Path nmPrivateContainerScriptPath = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
              + CONTAINER_SCRIPT);
      // 获取NM私有目录下令牌文件路径
      Path nmPrivateTokensPath = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
              + String.format(TOKEN_FILE_NAME_FMT, containerIdStr));
      // 获取NM私有目录下keystore路径
      Path nmPrivateKeystorePath = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
              + KEYSTORE_FILE);
      // 获取NM私有目录下truststore路径
      Path nmPrivateTruststorePath = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
              + TRUSTSTORE_FILE);
      Path nmPrivateClasspathJarDir = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr));

      // 确定容器工作目录
      Path containerWorkDir = deriveContainerWorkDir();
      // 记录容器工作目录到NM状态存储
      recordContainerWorkDir(containerID, containerWorkDir.toString());

      // 确定容器CSI卷挂载根目录
      Path csiVolumesRoot = deriveCsiVolumesRootDir();
      // 记录CSI卷根目录到容器信息
      recordContainerCsiVolumesRootDir(containerID, csiVolumesRoot.toString());

      String pidFileSubpath = getPidFileSubpath(appIdStr, containerIdStr);
      // pid文件存放在NM私有目录，避免用户直接访问
      pidFilePath = dirsHandler.getLocalPathForWrite(pidFileSubpath);
      List<String> localDirs = dirsHandler.getLocalDirs();
      List<String> localDirsForRead = dirsHandler.getLocalDirsForRead();
      List<String> logDirs = dirsHandler.getLogDirs();
      List<String> filecacheDirs = getNMFilecacheDirs(localDirsForRead);
      List<String> userLocalDirs = getUserLocalDirs(localDirs);
      List<String> containerLocalDirs = getContainerLocalDirs(localDirs);
      List<String> containerLogDirs = getContainerLogDirs(logDirs);
      List<String> userFilecacheDirs = getUserFilecacheDirs(localDirsForRead);
      List<String> applicationLocalDirs = getApplicationLocalDirs(localDirs,
          appIdStr);

      // 检查磁盘健康状态，大部分磁盘失败则直接返回磁盘失败错误
      if (!dirsHandler.areDisksHealthy()) {
        ret = ContainerExitStatus.DISKS_FAILED;
        throw new IOException("Most of the disks failed. "
            + dirsHandler.getDisksHealthReport(false));
      }
      // 收集所有本地目录下的应用目录路径
      List<Path> appDirs = new ArrayList<Path>(localDirs.size());
      for (String localDir : localDirs) {
        Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
        Path userdir = new Path(usersdir, user);
        Path appsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
        appDirs.add(new Path(appsdir, appIdStr));
      }

      // 如果存在keystore，写入NM私有目录
      byte[] keystore = container.getCredentials().getSecretKey(
          AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE);
      if (keystore != null) {
        try (DataOutputStream keystoreOutStream =
                 lfs.create(nmPrivateKeystorePath,
                     EnumSet.of(CREATE, OVERWRITE))) {
          keystoreOutStream.write(keystore);
        }
      } else {
        nmPrivateKeystorePath = null;
      }
      // 如果存在truststore，写入NM私有目录