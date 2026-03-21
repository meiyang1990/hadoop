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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerExecCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerKillCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerPullCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRmCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerStartCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.volume.csi.ContainerVolumePublisher;
import org.apache.hadoop.yarn.util.DockerClientConfigHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerClient;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerInspectCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

/**
 * Docker容器运行时实现，继承自OCI容器运行时，通过PrivilegedOperationExecutor使用原生container-executor二进制
 * 在Docker容器内部启动YARN容器进程，支持通过环境变量配置Docker引擎各项参数。
 *
 * <p>以下环境变量用于配置Docker容器：</p>
 *
 * <ul>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_TYPE} 决定是否使用Docker容器。值为{@code docker}时使用Docker，否则使用普通进程树容器。
 *     由{@link DelegatingLinuxContainerRuntime}调用{@link #isDockerContainerRequested}方法检查该变量。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_IMAGE} 指定用于启动Docker容器的镜像名称。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE} 控制是否覆盖Docker容器默认命令。
 *     设为{@code true}时，容器命令为{@code bash <path_to_launch_script>}；未设置或设为{@code false}时使用容器默认命令。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK} 设置Docker容器使用的网络类型，必须是
 *     {@code yarn.nodemanager.runtime.linux.docker.allowed-container-networks}配置允许的值。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_PORTS_MAPPING} 为桥接网络模式的Docker容器指定端口映射。
 *     值为逗号分隔的端口映射列表，格式同Docker run的-p选项。值为空时添加-P选项。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_PID_NAMESPACE} 控制Docker容器使用的PID命名空间。
 *     默认每个容器有自己的PID命名空间。要共享宿主机PID命名空间，需要将
 *     {@code yarn.nodemanager.runtime.linux.docker.host-pid-namespace.allowed}设为{@code true}，
 *     且该环境变量设为{@code host}，不允许其他值。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME} 设置Docker容器的主机名。未指定时，
 *     非host网络模式会使用容器ID生成默认主机名。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER} 控制是否启用特权容器。
 *     要使用特权容器，需要将{@code yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed}
 *     设为{@code true}，且应用所有者在{@code yarn.nodemanager.runtime.linux.docker.privileged-containers.acl}
 *     配置的白名单中。该环境变量设为{@code true}时，允许则启用特权容器，不允许其他值，不使用应保留未设置而非设为false。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS} 允许用户为Docker容器指定额外挂载卷。
 *     值为逗号分隔的挂载列表，格式为{@code source:dest[:mode]}，mode只能是ro(只读)或rw(读写)，
 *     未指定默认为rw。mode可包含绑定传播选项，格式为[option]、rw+[option]或ro+[option]，
 *     合法选项包括shared、rshared、slave、rslave、private、rprivate。请求的挂载会由container-executor
 *     根据container-executor.cfg中docker.allowed.ro-mounts和docker.allowed.rw-mounts配置校验。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_TMPFS_MOUNTS} 允许用户为Docker容器指定额外tmpfs挂载，
 *     值为逗号分隔的挂载列表。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL} 允许用户针对单个容器请求延迟删除Docker容器。
 *     设为true时，Docker容器会等到{@code yarn.nodemanager.delete.debug-delay-sec}配置的时长过去后再删除，
 *     管理员可通过yarn-site配置{@code yarn.nodemanager.runtime.linux.docker.delayed-removal.allowed}禁用该功能，
 *     默认禁用。功能禁用或设为false时，容器退出后立即删除。
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_YARN_SYSFS_ENABLE} 允许将YARN服务json导出到Docker容器，默认禁用。
 *     启用后，容器内可通过/hadoop/yarn/sysfs/app.json访问app.json。
 *   </li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DockerLinuxContainerRuntime extends OCIContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(DockerLinuxContainerRuntime.class);

  // Docker镜像名称正则校验 pattern
  public static final String DOCKER_IMAGE_PATTERN =
      "^(([a-zA-Z0-9.-]+)(:\\d+)?/)?([a-z0-9_./-]+)(:[\\w.-]+)?$";
  private static final Pattern dockerImagePattern =
      Pattern.compile(DOCKER_IMAGE_PATTERN);

  private static final Pattern DOCKER_DIGEST_PATTERN = Pattern.compile("^sha256:[a-z0-9]{12,64}$");

  private static final String DEFAULT_PROCFS = "/proc";

  @InterfaceAudience.Private
  private static final String RUNTIME_TYPE = "DOCKER";

  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_IMAGE =
      "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_CLIENT_CONFIG =
      "YARN_CONTAINER_RUNTIME_DOCKER_CLIENT_CONFIG";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_NETWORK =
      "YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_HOSTNAME =
      "YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_MOUNTS =
      "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_TMPFS_MOUNTS =
      "YARN_CONTAINER_RUNTIME_DOCKER_TMPFS_MOUNTS";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_DELAYED_REMOVAL =
      "YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_PORTS_MAPPING =
      "YARN_CONTAINER_RUNTIME_DOCKER_PORTS_MAPPING";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_YARN_SYSFS =
      "YARN_CONTAINER_RUNTIME_YARN_SYSFS_ENABLE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_DOCKER_RUNTIME =
      "YARN_CONTAINER_RUNTIME_DOCKER_RUNTIME";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_DOCKER_SERVICE_MODE =
      "YARN_CONTAINER_RUNTIME_DOCKER_SERVICE_MODE";

  @InterfaceAudience.Private
  public final static String ENV_OCI_CONTAINER_PID_NAMESPACE =
      formatOciEnvKey(RUNTIME_TYPE, CONTAINER_PID_NAMESPACE_SUFFIX);
  @InterfaceAudience.Private
  public final static String ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER =
      formatOciEnvKey(RUNTIME_TYPE, RUN_PRIVILEGED_CONTAINER_SUFFIX);

  private Configuration conf;
  private Context nmContext;
  private DockerClient dockerClient;
  private PrivilegedOperationExecutor privilegedOperationExecutor;
  private String defaultImageName;
  private Boolean defaultImageUpdate;
  private Set<String> allowedNetworks = new HashSet<>();
  private Set<String> allowedRuntimes = new HashSet<>();
  private String defaultNetwork;
  private CGroupsHandler cGroupsHandler;
  private AccessControlList privilegedContainersAcl;
  private boolean enableUserReMapping;
  private int userRemappingUidThreshold;
  private int userRemappingGidThreshold;
  private Set<String> capabilities;
  private boolean delayedRemovalAllowed;
  private Set<String> defaultROMounts = new HashSet<>();
  private Set<String> defaultRWMounts = new HashSet<>();
  private Set<String> defaultTmpfsMounts = new HashSet<>();

  /**
   * 检查环境变量是否请求使用Docker容器。
   * @param daemonConf NodeManager守护进程配置
   * @param env 操作的环境变量设置
   * @return 是否请求使用Docker容器
   */
  public static boolean isDockerContainerRequested(Configuration daemonConf,
      Map<String, String> env) {
    String type = (env == null)
        ? null : env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);
    if (type == null) {
      type = daemonConf.get(YarnConfiguration.LINUX_CONTAINER_RUNTIME_TYPE);
    }
    return type != null && type.equals(
        ContainerRuntimeConstants.CONTAINER_RUNTIME_DOCKER);
  }

  /**
   * 使用指定的PrivilegedOperationExecutor创建DockerLinuxContainerRuntime实例。
   * @param privilegedOperationExecutor 用于执行特权操作的执行器实例
   */
  public DockerLinuxContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this(privilegedOperationExecutor,
        ResourceHandlerModule.getCGroupsHandler());
  }

  /**
   * 使用指定的特权操作执行器和cgroups处理器创建实例，供测试使用。
   * @param privilegedOperationExecutor 特权操作执行器实例
   * @param cGroupsHandler cgroups处理器实例
   */
  @VisibleForTesting
  public DockerLinuxContainerRuntime(
      PrivilegedOperationExecutor privilegedOperationExecutor,
      CGroupsHandler cGroupsHandler) {
    super(privilegedOperationExecutor, cGroupsHandler);

    this.privilegedOperationExecutor = privilegedOperationExecutor;

    if (cGroupsHandler == null) {
      LOG.info("cGroupsHandler is null - cgroups not in use.");
    } else {
      this.cGroupsHandler = cGroupsHandler;
    }
  }

  @Override
  public void initialize(Configuration conf, Context nmContext)
      throws ContainerExecutionException {
    super.initialize(conf, nmContext);
    this.nmContext = nmContext;
    this.conf = conf;

    // 初始化Docker客户端
    dockerClient = new DockerClient();
    // 清空各配置集合
    allowedNetworks.clear();
    allowedRuntimes.clear();
    defaultROMounts.clear();
    defaultRWMounts.clear();
    defaultTmpfsMounts.clear();
    // 加载默认镜像配置
    defaultImageName = conf.getTrimmed(
        YarnConfiguration.NM_DOCKER_IMAGE_NAME, "");
    defaultImageUpdate = conf.getBoolean(
        YarnConfiguration.NM_DOCKER_IMAGE_UPDATE, false);
    // 加载允许的网络配置
    allowedNetworks.addAll(Arrays.asList(
        conf.getTrimmedStrings(
            YarnConfiguration.NM_DOCKER_ALLOWED_CONTAINER_NETWORKS,
            YarnConfiguration.DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_NETWORKS)));
    defaultNetwork = conf.getTrimmed(
        YarnConfiguration.NM_DOCKER_DEFAULT_CONTAINER