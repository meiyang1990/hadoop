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

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.APPID;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.APPLICATION_LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_ID_STR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_LAUNCH_PREFIX_COMMANDS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_LOG_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_RUN_CMDS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_WORK_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.FILECACHE_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCALIZED_RESOURCES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOG_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_CONTAINER_SCRIPT_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_KEYSTORE_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_TOKENS_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_TRUSTSTORE_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.PID;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.PID_FILE_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.RESOURCES_OPTIONS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.RUN_AS_USER;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.SIGNAL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.TC_COMMAND_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER_FILECACHE_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER_LOCAL_DIRS;

import org.apache.hadoop.classification.VisibleForTesting;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DefaultLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DelegatingLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.OCIContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRmCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerPrepareContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Linux平台容器执行器，使用原生container-executor二进制文件启动容器。通过原生代码助手，
 * 本类可以实现DefaultContainerExecutor无法完成的多项功能，比如以应用提交用户身份执行应用、
 * 支持将应用用户映射到宿主机UID进行本地化、通过Linux CGROUPS进行资源管理，以及Docker容器支持。</p>
 *
 * <p>如果{@code hadoop.security.authentication}设置为{@code simple}，
 * 则{@code yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users}
 * 配置项决定LinuxContainerExecutor是以应用用户身份还是以默认用户运行进程，
 * 默认用户由{@code yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user}
 * 配置项指定。</p>
 *
 * <p>LinuxContainerExecutor通过合适的{@link LinuxContainerRuntime}实例管理应用。
 * 本类使用{@link DelegatingLinuxContainerRuntime}实例，会根据作业配置将调用转发给
 * {@link DefaultLinuxContainerRuntime}实例或{@link OCIContainerRuntime}实例。</p>
 *
 * @see LinuxContainerRuntime
 * @see DelegatingLinuxContainerRuntime
 * @see DefaultLinuxContainerRuntime
 * @see DockerLinuxContainerRuntime
 * @see OCIContainerRuntime#isOCICompliantContainerRequested
 */
/**
 * YARN NodeManager Linux平台容器执行器，基于原生container-executor实现容器生命周期管理
 */
public class LinuxContainerExecutor extends ContainerExecutor {

  private static final Logger LOG =
       LoggerFactory.getLogger(LinuxContainerExecutor.class);

  private String nonsecureLocalUser;
  private Pattern nonsecureLocalUserPattern;
  private LCEResourcesHandler resourcesHandler;
  private boolean containerSchedPriorityIsSet = false;
  private int containerSchedPriorityAdjustment = 0;
  private boolean containerLimitUsers;
  private ResourceHandler resourceHandlerChain;
  private LinuxContainerRuntime linuxContainerRuntime;
  private Context nmContext;

  /**
   * 容器执行器退出码定义，对应原生container-executor的退出码
   */
  public enum ExitCode {
    SUCCESS(0),
    INVALID_ARGUMENT_NUMBER(1),
    INVALID_COMMAND_PROVIDED(3),
    INVALID_NM_ROOT_DIRS(5),
    SETUID_OPER_FAILED(6),
    UNABLE_TO_EXECUTE_CONTAINER_SCRIPT(7),
    UNABLE_TO_SIGNAL_CONTAINER(8),
    INVALID_CONTAINER_PID(9),
    OUT_OF_MEMORY(18),
    INITIALIZE_USER_FAILED(20),
    PATH_TO_DELETE_IS_NULL(21),
    INVALID_CONTAINER_EXEC_PERMISSIONS(22),
    INVALID_CONFIG_FILE(24),
    SETSID_OPER_FAILED(25),
    WRITE_PIDFILE_FAILED(26),
    WRITE_CGROUP_FAILED(27),
    TRAFFIC_CONTROL_EXECUTION_FAILED(28),
    DOCKER_RUN_FAILED(29),
    ERROR_OPENING_DOCKER_FILE(30),
    ERROR_READING_DOCKER_FILE(31),
    FEATURE_DISABLED(32),
    COULD_NOT_CREATE_SCRIPT_COPY(33),
    COULD_NOT_CREATE_CREDENTIALS_FILE(34),
    COULD_NOT_CREATE_WORK_DIRECTORIES(35),
    COULD_NOT_CREATE_APP_LOG_DIRECTORIES(36),
    COULD_NOT_CREATE_TMP_DIRECTORIES(37),
    ERROR_CREATE_CONTAINER_DIRECTORIES_ARGUMENTS(38),
    CANNOT_GET_EXECUTABLE_NAME_FROM_READLINK(80),
    TOO_LONG_EXECUTOR_PATH(81),
    CANNOT_GET_EXECUTABLE_NAME_FROM_KERNEL(82),
    CANNOT_GET_EXECUTABLE_NAME_FROM_PID(83),
    WRONG_PATH_OF_EXECUTABLE(84);
    private final int code;

    ExitCode(int exitCode) {
      this.code = exitCode;
    }

    /**
     * 获取退出码整数值.
     * @return 退出码整数值
     */
    public int getExitCode() {
      return code;
    }

    @Override
    public String toString() {
      return String.valueOf(code);
    }
  }

  /**
   * 默认构造函数，支持反射创建实例
   */
  public LinuxContainerExecutor() {
  }

  /**
   * 带指定容器运行时的构造函数，主要用于测试
   *
   * @param linuxContainerRuntime 要使用的容器运行时
   */
  public LinuxContainerExecutor(LinuxContainerRuntime linuxContainerRuntime) {
    this.linuxContainerRuntime = linuxContainerRuntime;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);

    // 获取配置指定的资源处理器
    resourcesHandler = getResourcesHandler(conf);

    containerSchedPriorityIsSet = false;
    // 检查是否配置了容器调度优先级调整
    if (conf.get(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY)
        != null) {
      containerSchedPriorityIsSet = true;
      containerSchedPriorityAdjustment = conf
          .getInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY,
              YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY);
    }
    // 加载非安全模式默认运行用户配置
    nonsecureLocalUser = conf.get(
        YarnConfiguration.NM_NONSECURE_MODE_LOCAL_USER_KEY,
        YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER);
    // 加载非安全模式用户名正则匹配规则
    nonsecureLocalUserPattern = Pattern.compile(
        conf.get(YarnConfiguration.NM_NONSECURE_MODE_USER_PATTERN_KEY,
            YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_USER_PATTERN));
    // 加载非安全模式是否限制用户运行配置
    containerLimitUsers = conf.getBoolean(
        YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS,
        YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LIMIT_USERS);
    if (!containerLimitUsers) {
      LOG.warn("{}: impersonation without authentication enabled",
          YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS);
    }
  }

  /**
   * 根据配置获取资源处理器实例，已废弃旧的CgroupsLCEResourcesHandler，改用新的资源处理器链
   * @param conf 配置对象
   * @return 资源处理器实例
   */
  private LCEResourcesHandler getResourcesHandler(Configuration conf) {
    LCEResourcesHandler handler = ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.NM_LINUX_CONTAINER_RESOURCES_HANDLER,
            DefaultLCEResourcesHandler.class, LCEResourcesHandler.class), conf);

    // 停止使用旧的CgroupsLCEResourcesHandler，改用新的资源处理器链
    // ResourceHandlerModule会在需要时自动创建cgroup cpu模块
    if (handler instanceof CgroupsLCEResourcesHandler) {
      handler =
          ReflectionUtils.newInstance(DefaultLCEResourcesHandler.class, conf);
    }
    handler.setConf(conf);
    return handler;
  }

  /**
   * 在非安全模式下验证用户名是否匹配配置的正则模式
   * @param user 待验证用户名
   */
  void verifyUsernamePattern(String user) {
    if (!UserGroupInformation.isSecurityEnabled() &&
        !nonsecureLocalUserPattern.matcher(user).matches()) {
      throw new IllegalArgumentException("Invalid user name '" + user + "'," +
          " it must match '" + nonsecureLocalUserPattern.pattern() + "'");
    }
  }

  /**
   * 根据安全配置和用户限制规则，获取实际运行容器的用户名
   * @param user 原始提交用户
   * @return 实际运行用户名
   */
  String getRunAsUser(String user) {
    if (UserGroupInformation.isSecurityEnabled() ||
        !containerLimitUsers) {
      return user;
    } else {
      return nonsecureLocalUser;
    }
  }

  /**
   * 获取原生container-executor二进制文件的绝对路径
   *
   * @param conf 配置对象
   * @return container-executor二进制文件的绝对路径
   */
  protected String getContainerExecutorExecutablePath(Configuration conf) {
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

  /**
   * 向命令行添加nice命令调整容器进程调度优先级，使用配置中指定的nice值
   *
   * @param command 待调整的命令行列表
   */
  protected void addSchedPriorityCommand(List<String> command) {
    if (containerSchedPriorityIsSet) {
      command.addAll(Arrays.asList("nice", "-n",
          Integer.toString(containerSchedPriorityAdjustment