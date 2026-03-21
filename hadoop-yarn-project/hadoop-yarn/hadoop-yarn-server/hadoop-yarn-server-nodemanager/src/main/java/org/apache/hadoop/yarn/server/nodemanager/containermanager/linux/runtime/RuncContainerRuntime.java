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
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageManifest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCILayer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig.OCILinuxConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig.OCIMount;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig.OCIProcessConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncImageTagToManifestPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncManifestToResourcesPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizedResource;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.volume.csi.ContainerVolumePublisher;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_TAG_TO_MANIFEST_PLUGIN;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_LAYER_MOUNTS_TO_KEEP;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_REAP_RUNC_LAYER_MOUNTS_INTERVAL;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_MANIFEST_TO_RESOURCES_PLUGIN;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TAG_TO_MANIFEST_PLUGIN;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_LAYER_MOUNTS_TO_KEEP;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_MANIFEST_TO_RESOURCES_PLUGIN;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_REAP_RUNC_LAYER_MOUNTS_INTERVAL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;
/**
 * Runc容器运行时实现，基于OCI标准使用runc工具启动隔离容器，继承自OCIContainerRuntime。
 * 通过PrivilegedOperationExecutor调用原生container-executor二进制，在Runc容器内启动YARN容器进程。
 * 支持用户通过环境变量配置容器镜像、挂载点、主机名等参数。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RuncContainerRuntime extends OCIContainerRuntime {

  private static final Logger LOG = LoggerFactory.getLogger(RuncContainerRuntime.class);

  @InterfaceAudience.Private
  private static final String RUNTIME_TYPE = "RUNC";

  @InterfaceAudience.Private
  public static final String ENV_RUNC_CONTAINER_IMAGE =
      "YARN_CONTAINER_RUNTIME_RUNC_IMAGE";
  @InterfaceAudience.Private
  public static final String ENV_RUNC_CONTAINER_MOUNTS =
      "YARN_CONTAINER_RUNTIME_RUNC_MOUNTS";
  @InterfaceAudience.Private
  public static final String ENV_RUNC_CONTAINER_HOSTNAME =
      "YARN_CONTAINER_RUNTIME_RUNC_CONTAINER_HOSTNAME";

  @InterfaceAudience.Private
  public final static String ENV_RUNC_CONTAINER_PID_NAMESPACE =
      formatOciEnvKey(RUNTIME_TYPE, CONTAINER_PID_NAMESPACE_SUFFIX);
  @InterfaceAudience.Private
  public final static String ENV_RUNC_CONTAINER_RUN_PRIVILEGED_CONTAINER =
      formatOciEnvKey(RUNTIME_TYPE, RUN_PRIVILEGED_CONTAINER_SUFFIX);

  private Configuration conf;
  private Context nmContext;
  private PrivilegedOperationExecutor privilegedOperationExecutor;
  private CGroupsHandler cGroupsHandler;
  private RuncImageTagToManifestPlugin imageTagToManifestPlugin;
  private RuncManifestToResourcesPlugin manifestToResourcesPlugin;
  private ObjectMapper mapper;
  private String seccomp;
  private int layersToKeep;
  private String defaultRuncImage;
  private ScheduledExecutorService exec;
  private String seccompProfile;
  private Set<String> defaultROMounts = new HashSet<>();
  private Set<String> defaultRWMounts = new HashSet<>();
  private Set<String> allowedNetworks = new HashSet<>();
  private Set<String> allowedRuntimes = new HashSet<>();
  private AccessControlList privilegedContainersAcl;

  /**
   * 构造函数使用默认CGroupsHandler。
   */
  public RuncContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this(privilegedOperationExecutor, ResourceHandlerModule
        .getCGroupsHandler());
  }

  /**
   * 带注入CGroupsHandler的构造函数，主要用于测试。
   */
  @VisibleForTesting
  public RuncContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor, CGroupsHandler cGroupsHandler) {
    super(privilegedOperationExecutor, cGroupsHandler);
    this.privilegedOperationExecutor = privilegedOperationExecutor;

    if (cGroupsHandler == null) {
      LOG.info("cGroupsHandler is null - cgroups not in use.");
    } else {
      this.cGroupsHandler = cGroupsHandler;
    }
  }

  @Override
  public void initialize(Configuration configuration, Context nmCtx)
      throws ContainerExecutionException {
    super.initialize(configuration, nmCtx);
    this.conf = configuration;
    this.nmContext = nmCtx;
    // 加载并初始化镜像标签转清单插件
    imageTagToManifestPlugin = chooseImageTagToManifestPlugin();
    imageTagToManifestPlugin.init(conf);
    // 加载并初始化清单转资源插件
    manifestToResourcesPlugin = chooseManifestToResourcesPlugin();
    manifestToResourcesPlugin.init(conf);
    // 初始化JSON序列化工具
    mapper = new ObjectMapper();
    // 获取默认Runc镜像配置
    defaultRuncImage = conf.get(YarnConfiguration.NM_RUNC_IMAGE_NAME);

    // 清空允许的网络和运行时列表
    allowedNetworks.clear();
    allowedRuntimes.clear();

    // 加载配置允许的容器网络
    allowedNetworks.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_RUNC_ALLOWED_CONTAINER_NETWORKS,
        YarnConfiguration.DEFAULT_NM_RUNC_ALLOWED_CONTAINER_NETWORKS)));

    // 加载配置允许的容器运行时
    allowedRuntimes.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_RUNC_ALLOWED_CONTAINER_RUNTIMES,
        YarnConfiguration.DEFAULT_NM_RUNC_ALLOWED_CONTAINER_RUNTIMES)));

    // 加载特权容器访问控制列表
    privilegedContainersAcl = new AccessControlList(conf.getTrimmed(
        YarnConfiguration.NM_RUNC_PRIVILEGED_CONTAINERS_ACL,
        YarnConfiguration.DEFAULT_NM_RUNC_PRIVILEGED_CONTAINERS_ACL));

    // 读取seccomp配置文件路径
    seccompProfile = conf.get(YarnConfiguration.NM_RUNC_SECCOMP_PROFILE);

    // 加载默认只读挂载点
    defaultROMounts.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_RUNC_DEFAULT_RO_MOUNTS)));

    // 加载默认可写挂载点
    defaultRWMounts.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_RUNC_DEFAULT_RW_MOUNTS)));

    // 读取seccomp配置文件内容
    try {
      if (seccompProfile != null) {
        seccomp = new String(Files.readAllBytes(Paths.get(seccompProfile)),
            StandardCharsets.UTF_8);
      }
    } catch (IOException ioe) {
      throw new ContainerExecutionException(ioe);
    }

    // 获取需要保留的镜像层数配置
    layersToKeep = conf.getInt(NM_RUNC_LAYER_MOUNTS_TO_KEEP,
        DEFAULT_NM_RUNC_LAYER_MOUNTS_TO_KEEP);

  }

  @Override
  public void start() {
    // 获取过期层回收间隔配置
    int reapRuncLayerMountsInterval =
        conf.getInt(NM_REAP_RUNC_LAYER_MOUNTS_INTERVAL,
        DEFAULT_NM_REAP_RUNC_LAYER_MOUNTS_INTERVAL);
    // 创建定时线程池执行过期层回收
    exec = HadoopExecutors.newScheduledThreadPool(1);
    exec.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            try {
              // 创建回收过期runc层挂载的特权操作
              PrivilegedOperation launchOp = new PrivilegedOperation(
                  PrivilegedOperation.OperationType.REAP_RUNC_LAYER_MOUNTS);
              launchOp.appendArgs(Integer.toString(layersToKeep));
              try {
                // 执行特权操作回收过期层
                String stdout = privilegedOperationExecutor
                    .executePrivilegedOperation(null,
                    launchOp, null, null, false, false);
                if(stdout != null) {
                  LOG.info("Reap layer mounts thread: " + stdout);
                }
              } catch (PrivilegedOperationException e) {
                LOG.warn("Failed to reap old runc layer mounts", e);
              }
            } catch (Exception e) {
              LOG.warn("Reap layer mount thread caught an exception: ", e);
            }
          }
        }, 0, reapRuncLayerMountsInterval, TimeUnit.SECONDS);
    // 启动两个插件
    imageTagToManifestPlugin.start();
    manifestToResourcesPlugin.start();
  }

  @Override
  public void stop() {
    // 关闭定时线程池
    exec.shutdownNow();
    // 停止两个插件
    imageTagToManifestPlugin.stop();
    manifestToResourcesPlugin.stop();
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    List<String> env = new ArrayList<>();
    Container container = ctx.getContainer();
    String runAsUser = ctx.getExecutionAttribute(RUN_AS_USER);
    String user = ctx.getExecutionAttribute(USER);
    ContainerId containerId = container.getContainerId();
    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();

    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    ArrayList<OCIMount> mounts = new ArrayList<>();
    ArrayList<OCILayer> layers = new ArrayList<>();
    String hostname = environment.get(ENV_RUNC_CONTAINER_HOSTNAME);

    // 验证主机名合法性
    validateHostname(hostname);

    String containerIdStr = containerId.toString();
    String applicationId = appId.toString();
    Path containerWorkDir = ctx.getExecutionAttribute(CONTAINER_WORK_DIR);

    // 获取容器运行时数据
    RuncRuntimeObject runcRuntimeObject =
        container.getContainerRuntimeData(RuncRuntimeObject.class);
    List<LocalResource> layerResources = runcRuntimeObject.getOCILayers();

    // 获取资源本地化服务
    ResourceLocalizationService localizationService =
        nmContext.getContainerManager().getResourceLocalizationService();

    List<String> args = new ArrayList<>();

    try {
      try {
        // 本地化容器配置文件
        LocalResource rsrc = runcRuntimeObject.getConfig();
        LocalResourceRequest req = new LocalResourceRequest(rsrc);
        LocalizedResource localRsrc = localizationService
            .getLocalizedResource(req, user, appId);
        if (localRsrc == null) {
          throw new ContainerExecutionException("Could not successfully " +
              "localize layers. rsrc: " + rsrc.getResource().getFile());
        }

        // 从配置文件提取环境变量
        File file = new File(localRsrc.getLocalPath().toString());
        List<String> imageEnv = extractImageEnv(file);
        if (imageEnv != null && !imageEnv.isEmpty()) {
          env.addAll(imageEnv);
        }
        // 从配置文件提取入口点
        List<String> entrypoint = extractImageEntrypoint(file);
        if (entrypoint != null && !entrypoint.isEmpty()) {
          args.addAll(entrypoint);
        }
      } catch (IOException ioe) {
        throw new ContainerExecutionException(ioe);
      }

      // 本地化所有OCI镜像层
      for (LocalResource rsrc : layerResources) {
        LocalResourceRequest req = new LocalResourceRequest(rsrc);
        LocalizedResource localRsrc = localizationService
            .getLocalizedResource(req, user, appId);

        OCILayer layer = new OCILayer("application/vnd.squashfs",
            localRsrc.getLocalPath().toString());
        layers.add(layer);
      }
    } catch (URISyntaxException e) {
      throw new ContainerExecutionException(e);
    }

    // 设置容器所有挂载点
    setContainerMounts(mounts, ctx, containerWorkDir, environment);

    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);

    Path nmPrivateContainerScriptPath = ctx.getExecutionAttribute(
        NM_PRIVATE_CONTAINER_SCRIPT_PATH);

    Path nmPrivateTokensPath =
        ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH);

    int cpuShares = container.getResource().getVirtualCores();

    // 矫正CPU份额，cgroups要求不小于2