// 这个文件已经全部加上中文注释
/*
 *
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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.impl.pb.client.CsiAdaptorProtocolPBClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.util.csi.CsiConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime.isDockerContainerRequested;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.RuncContainerRuntime.isRuncContainerRequested;

/**
 * OCI 标准容器运行时抽象基类，基于特权操作执行器启动符合 OCI 标准的容器进程，
 * 是 Docker 和 Runc 容器运行时的公共父类，提供通用的容器校验和初始化能力。
 *
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class OCIContainerRuntime implements LinuxContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(OCIContainerRuntime.class);

  // 主机名校验正则表达式，符合 OCI 规范
  private static final Pattern HOSTNAME_PATTERN = Pattern.compile(
      "^[a-zA-Z0-9][a-zA-Z0-9_.-]+$");
  // 用户自定义挂载解析正则表达式
  static final Pattern USER_MOUNT_PATTERN = Pattern.compile(
      "(?<=^|,)([^:\\x00]+):([^:\\x00]+)" +
      "(:(r[ow]|(r[ow][+])?(r?shared|r?slave|r?private)))?(?:,|$)");
  // tmpfs 挂载解析正则表达式
  static final Pattern TMPFS_MOUNT_PATTERN = Pattern.compile(
      "^/[^:\\x00]+$");
  // 端口映射规则正则表达式
  private static final String PORTS_MAPPING_PATTERN =
      "^:[0-9]+|^[0-9]+:[0-9]+|^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]" +
      "|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])" +
      ":[0-9]+:[0-9]+$";
  // 主机名最大长度限制
  private static final int HOST_NAME_LENGTH = 64;

  @InterfaceAudience.Private
  public static final String RUNTIME_PREFIX = "YARN_CONTAINER_RUNTIME_%s_%s";
  @InterfaceAudience.Private
  public static final String CONTAINER_PID_NAMESPACE_SUFFIX =
      "CONTAINER_PID_NAMESPACE";
  @InterfaceAudience.Private
  public static final String RUN_PRIVILEGED_CONTAINER_SUFFIX =
      "RUN_PRIVILEGED_CONTAINER";

  // CSI 适配器客户端缓存，按驱动名称存储
  private Map<String, CsiAdaptorProtocol> csiClients = new HashMap<>();

  /**
   * 获取允许使用的容器网络类型列表。
   * @return 允许的网络类型集合
   */
  abstract Set<String> getAllowedNetworks();

  /**
   * 获取允许使用的OCI运行时列表。
   * @return 允许的运行时集合
   */
  abstract Set<String> getAllowedRuntimes();

  /**
   * 获取集群是否启用主机PID namespace功能。
   * @return true 启用，false 禁用
   */
  abstract boolean getHostPidNamespaceEnabled();

  /**
   * 获取集群是否允许特权容器运行。
   * @return true 允许，false 禁止
   */
  abstract boolean getPrivilegedContainersEnabledOnCluster();

  /**
   * 获取允许运行特权容器的ACL控制列表。
   * @return 特权容器ACL
   */
  abstract AccessControlList getPrivilegedContainersAcl();

  /**
   * 获取PID namespace相关环境变量名。
   * @return 环境变量名
   */
  abstract String getEnvOciContainerPidNamespace();

  /**
   * 获取特权容器相关环境变量名。
   * @return 环境变量名
   */
  abstract String getEnvOciContainerRunPrivilegedContainer();

  public OCIContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this(privilegedOperationExecutor, ResourceHandlerModule
        .getCGroupsHandler());
  }

  public OCIContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor, CGroupsHandler cGroupsHandler) {
  }

  /**
   * 初始化OCI容器运行时。
   * @param conf 配置对象
   * @param nmContext NodeManager上下文
   * @throws ContainerExecutionException 初始化异常
   */
  public void initialize(Configuration conf, Context nmContext)
      throws ContainerExecutionException {

  }

  /**
   * 判断当前容器是否请求使用OCI兼容容器运行。
   * @param daemonConf NodeManager配置
   * @param env 容器环境变量
   * @return true 如果请求Docker或Runc容器
   */
  public static boolean isOCICompliantContainerRequested(
      Configuration daemonConf, Map<String, String> env) {
    return isDockerContainerRequested(daemonConf, env) ||
        isRuncContainerRequested(daemonConf, env);
  }

  @VisibleForTesting
  protected String mountReadOnlyPath(String mount,
      Map<Path, List<String>> localizedResources)
      throws ContainerExecutionException {
    // 遍历所有本地化资源匹配挂载请求
    for (Map.Entry<Path, List<String>> resource :
        localizedResources.entrySet()) {
      if (resource.getValue().contains(mount)) {
        java.nio.file.Path path = Paths.get(resource.getKey().toString());
        // 校验路径为绝对路径
        if (!path.isAbsolute()) {
          throw new ContainerExecutionException("Mount must be absolute: " +
              mount);
        }
        // 禁止挂载符号链接避免安全问题
        if (Files.isSymbolicLink(path)) {
          throw new ContainerExecutionException("Mount cannot be a symlink: " +
              mount);
        }
        return path.toString();
      }
    }
    // 挂载路径必须是已经本地化的资源
    throw new ContainerExecutionException("Mount must be a localized " +
        "resource: " + mount);
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
  }

  /**
   * 通过系统id命令获取指定用户的UID。
   * @param userName 用户名
   * @return UID字符串
   * @throws ContainerExecutionException 命令执行失败抛出异常
   */
  protected String getUserIdInfo(String userName)
      throws ContainerExecutionException {
    String id;
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
        new String[]{"id", "-u", userName});
    try {
      shexec.execute();
      id = shexec.getOutput().replaceAll("[^0-9]", "");
    } catch (Exception e) {
      throw new ContainerExecutionException(e);
    }
    return id;
  }

  /**
   * 通过系统id命令获取指定用户的所有GID。
   * @param userName 用户名
   * @return GID字符串数组
   * @throws ContainerExecutionException 命令执行失败抛出异常
   */
  protected String[] getGroupIdInfo(String userName)
      throws ContainerExecutionException {
    String[] id;
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
        new String[]{"id", "-G", userName});
    try {
      shexec.execute();
      id = shexec.getOutput().replace("\n", "").split(" ");
    } catch (Exception e) {
      throw new ContainerExecutionException(e);
    }
    return id;
  }

  /**
   * 校验容器请求的网络类型是否在允许列表中。
   * @param network 请求的网络类型
   * @throws ContainerExecutionException 不允许时抛出异常
   */
  protected void validateContainerNetworkType(String network)
      throws ContainerExecutionException {
    Set<String> allowedNetworks = getAllowedNetworks();
    if (allowedNetworks.contains(network)) {
      return;
    }

    String msg = "Disallowed network:  '" + network
        + "' specified. Allowed networks: are " + allowedNetworks
        .toString();
    throw new ContainerExecutionException(msg);
  }

  /**
   * 校验容器请求的运行时是否在允许列表中。
   * @param runtime 请求的运行时名称
   * @throws ContainerExecutionException 不允许时抛出异常
   */
  protected void validateContainerRuntimeType(String runtime)
      throws ContainerExecutionException {
    Set<String> allowedRuntimes = getAllowedRuntimes();
    if (runtime == null || runtime.isEmpty()
        || allowedRuntimes.contains(runtime)) {
      return;
    }

    String msg = "Disallowed runtime:  '" + runtime
            + "' specified. Allowed runtimes: are " + allowedRuntimes
            .toString();
    throw new ContainerExecutionException(msg);
  }

  /**
   * 判断是否允许容器使用主机PID namespace。
   * 需要同时满足用户请求该功能且集群开启该功能两个条件。
   *
   * @param container YARN容器对象
   * @return true 允许使用主机PID namespace
   * @throws ContainerExecutionException 请求了但不允许时抛出异常
   */
  protected boolean allowHostPidNamespace(Container container)
      throws ContainerExecutionException {
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String envOciContainerPidNamespace = getEnvOciContainerPidNamespace();

    String pidNamespace = environment.get(envOciContainerPidNamespace);

    // 未请求该功能
    if (pidNamespace == null) {
      return false;
    }

    // 请求值不是host，不启用
    if (!pidNamespace.equalsIgnoreCase("host")) {
      LOG.warn("NOT requesting PID namespace. Value of " +
          envOciContainerPidNamespace
          + "is invalid: " + pidNamespace);
      return false;
    }

    boolean hostPidNamespaceEnabled = getHostPidNamespaceEnabled();

    // 用户请求但集群未开启功能，拒绝
    if (!hostPidNamespaceEnabled) {
      String message = "Host pid namespace being requested but this is not "
          + "enabled on this cluster";
      LOG.warn(message);
      throw new ContainerExecutionException(message);
    }

    return true;
  }


  /**
   * 校验容器主机名是否符合OCI规范要求。
   * @param hostname 待校验主机名
   * @throws ContainerExecutionException 不符合要求抛出异常
   */
  protected static void validateHostname(String hostname) throws
      ContainerExecutionException {
    if (hostname != null && !hostname.isEmpty()) {
      // 匹配正则格式
      if (!HOSTNAME_PATTERN.matcher(hostname).matches()) {
        throw new ContainerExecutionException("Hostname '" + hostname
            + "' doesn't match OCI-compliant hostname pattern");
      }
      // 校验长度限制
      if (hostname.length() > HOST_NAME_LENGTH) {
        throw new ContainerExecutionException(
            "Hostname can not be greater than " + HOST_NAME_LENGTH
                + " characters: " + hostname);
      }
    }
  }

  /**
   * 判断是否允许容器以特权模式运行。
   * 需要同时满足三个条件：用户主动请求、集群开启特权容器、提交用户在ACL白名单中。
   *
   * @param container YARN容器对象
   * @return true 允许特权容器运行
   * @throws ContainerExecutionException 请求了但不满足条件抛出异常
   */
  protected boolean allowPrivilegedContainerExecution(Container container)
      throws ContainerExecutionException {

    if(!isContainerRequestedAsPrivileged(container)) {
      return false;
    }

    LOG.info("Privileged container requested for : " + container
        .getContainerId().toString());

    // 校验1：集群是否开启特权容器功能
    boolean privilegedContainersEnabledOnCluster =
        getPrivilegedContainersEnabledOnCluster();

    if (!privilegedContainersEnabledOnCluster) {
      String message = "Privileged container being requested but privileged "
          + "containers are not enabled on this cluster";
      LOG.warn(message);
      throw new ContainerExecutionException(message);
    }

    // 校验2：提交用户是否在特权容器白名单中
    String submittingUser = container.getUser();
    UserGroupInformation submitterUgi = UserGroupInformation
        .createRemoteUser(submittingUser);

    if (!getPrivilegedContainersAcl().isUserAllowed(submitterUgi)) {
      String message = "Cannot launch privileged container. Submitting user ("
          + submittingUser + ") fails ACL check.";
      LOG.warn(message);
      throw new ContainerExecutionException(message);
    }

    LOG.info("All checks pass. Launching privileged container for : "
        + container.getContainerId().toString());

    return true;
  }

  /**
   * 仅判断容器是否请求了特权模式，不做权限校验。
   * @param container YARN容器对象
   * @return true 容器请求特权模式
   */
  protected boolean isContainerRequestedAsPrivileged(
      Container container) {
    String envOciContainerRunPrivilegedContainer =
        getEnvOciContainerRunPrivilegedContainer();
    String runPrivilegedContainerEnvVar = container.getLaunchContext()
        .getEnvironment().get(envOciContainerRunPrivilegedContainer);
    return Boolean.parseBoolean(runPrivilegedContainerEnvVar);
  }

  /**
   * 获取所有CSI适配器客户端缓存。
   * @return CSI客户端映射表
   */
  public Map<String, CsiAdaptorProtocol> getCsiClients() {
    return csiClients;
  }

   /**
   * 初始化本节点所有CSI驱动的适配器客户端，缓存到本地方便后续使用。
   * @param config 配置对象
   * @throws ContainerExecutionException 初始化失败抛出异常
   */
  protected void initiateCsiClients(Configuration config)
      throws ContainerExecutionException {
    String[] driverNames = CsiConfigUtils.getCsiDriverNames(config);
    if (driverNames != null && driverNames.length > 0) {
      // 遍历所有配置的CSI驱动创建客户端
      for (String driverName : driverNames) {
        try {
          // 从配置中获取对应驱动的CSI适配器地址
          InetSocketAddress adaptorServiceAddress =
              CsiConfigUtils.getCsiAdaptorAddressForDriver(driverName, config);
          LOG.info("Initializing a csi-adaptor-client for csi-adaptor {},"
              + " csi-driver {}", adaptorServiceAddress.toString(), driverName);
          // 创建Protobuf RPC客户端
          CsiAdaptorProtocolPBClientImpl client =
              new CsiAdaptorProtocolPBClientImpl(1L, adaptorServiceAddress,
                  config);
          //