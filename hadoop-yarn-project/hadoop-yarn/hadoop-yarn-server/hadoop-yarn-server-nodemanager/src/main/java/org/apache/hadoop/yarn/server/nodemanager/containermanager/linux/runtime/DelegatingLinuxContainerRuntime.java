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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 委托式Linux容器运行时，根据容器请求的类型选择对应具体运行时执行操作
 * 支持默认运行时、Docker、Java沙箱、Runc以及自定义可插拔运行时
 * 根据运行时声明的匹配规则自动路由请求到对应实现
 *
 * @see LinuxContainerRuntime#isRuntimeRequested
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegatingLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(DelegatingLinuxContainerRuntime.class);
  // 默认Linux容器运行时实例
  private DefaultLinuxContainerRuntime defaultLinuxContainerRuntime;
  // Docker容器运行时实例
  private DockerLinuxContainerRuntime dockerLinuxContainerRuntime;
  // Runc容器运行时实例
  private RuncContainerRuntime runcContainerRuntime;
  // Java沙箱容器运行时实例
  private JavaSandboxLinuxContainerRuntime javaSandboxLinuxContainerRuntime;
  // 允许启用的运行时类型集合
  private Set<String> allowedRuntimes = new HashSet<>();
  // 自定义可插拔运行时列表
  private List<LinuxContainerRuntime> pluggableRuntimes = new ArrayList<>();

  /**
   * 初始化所有允许的容器运行时实例
   * @param conf 配置对象
   * @param nmContext NodeManager上下文
   * @throws ContainerExecutionException 初始化失败抛出异常
   */
  @Override
  public void initialize(Configuration conf, Context nmContext)
      throws ContainerExecutionException {
    // 从配置读取允许的运行时列表
    String[] configuredRuntimes = conf.getTrimmedStrings(
        YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        YarnConfiguration.DEFAULT_LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES);
    // 遍历配置的运行时
    for (String configuredRuntime : configuredRuntimes) {
      String normRuntime = configuredRuntime.toUpperCase();
      // 添加到允许列表
      allowedRuntimes.add(normRuntime);
      // 如果是可插拔自定义运行时，创建并初始化
      if (isPluggableRuntime(normRuntime)) {
        LinuxContainerRuntime runtime = createPluggableRuntime(conf,
            configuredRuntime);
        runtime.initialize(conf, nmContext);
        pluggableRuntimes.add(runtime);
      }
    }
    // 初始化Java沙箱运行时（如果允许）
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.JAVASANDBOX.name())) {
      javaSandboxLinuxContainerRuntime = new JavaSandboxLinuxContainerRuntime(
          PrivilegedOperationExecutor.getInstance(conf));
      javaSandboxLinuxContainerRuntime.initialize(conf, nmContext);
    }
    // 初始化Docker运行时（如果允许）
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER.name())) {
      dockerLinuxContainerRuntime = new DockerLinuxContainerRuntime(
          PrivilegedOperationExecutor.getInstance(conf));
      dockerLinuxContainerRuntime.initialize(conf, nmContext);
    }
    // 初始化Runc运行时（如果允许）
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.RUNC.name())) {
      runcContainerRuntime = new RuncContainerRuntime(
          PrivilegedOperationExecutor.getInstance(conf));
      runcContainerRuntime.initialize(conf, nmContext);
    }
    // 初始化默认运行时（如果允许）
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT.name())) {
      defaultLinuxContainerRuntime = new DefaultLinuxContainerRuntime(
          PrivilegedOperationExecutor.getInstance(conf));
      defaultLinuxContainerRuntime.initialize(conf, nmContext);
    }
  }

  @Override
  public boolean isRuntimeRequested(Map<String, String> env) {
    return true;
  }

  /**
   * 根据容器环境选择匹配的容器运行时
   * 优先匹配Java沙箱，然后是Docker、Runc、可插拔运行时，最后回退到默认运行时
   * @param environment 容器环境变量
   * @return 匹配到的容器运行时
   * @throws ContainerExecutionException 没有匹配到允许的运行时抛出异常
   */
  @VisibleForTesting
  LinuxContainerRuntime pickContainerRuntime(
      Map<String, String> environment) throws ContainerExecutionException {
    LinuxContainerRuntime runtime;
    // 优先检查Java沙箱，确保Docker不会绕过沙箱控制
    if (javaSandboxLinuxContainerRuntime != null &&
        javaSandboxLinuxContainerRuntime.isRuntimeRequested(environment)){
      runtime = javaSandboxLinuxContainerRuntime;
    } else if (dockerLinuxContainerRuntime != null &&
        dockerLinuxContainerRuntime.isRuntimeRequested(environment)) {
      runtime = dockerLinuxContainerRuntime;
    } else if (runcContainerRuntime != null &&
        runcContainerRuntime.isRuntimeRequested(environment)) {
      runtime = runcContainerRuntime;
    } else {
      // 先查找可插拔自定义运行时
      LinuxContainerRuntime pluggableRuntime = pickPluggableRuntime(
          environment);
      if (pluggableRuntime != null) {
        runtime = pluggableRuntime;
      } else if (defaultLinuxContainerRuntime != null &&
          defaultLinuxContainerRuntime.isRuntimeRequested(environment)) {
        runtime = defaultLinuxContainerRuntime;
      } else {
        throw new ContainerExecutionException("Requested runtime not allowed.");
      }
    }

    LOG.debug("Using container runtime: {}", runtime.getClass()
          .getSimpleName());

    return runtime;
  }

  /**
   * 遍历可插拔运行时列表查找匹配的运行时
   * @param environment 容器环境变量
   * @return 第一个匹配到的可插拔运行时，没有则返回null
   */
  private LinuxContainerRuntime pickPluggableRuntime(
      Map<String, String> environment) {
    for (LinuxContainerRuntime runtime : pluggableRuntimes) {
      if (runtime.isRuntimeRequested(environment)) {
        return runtime;
      }
    }
    return null;
  }

  /**
   * 根据容器对象选择对应运行时
   * @param container 容器对象
   * @return 匹配到的容器运行时
   * @throws ContainerExecutionException 匹配失败抛出异常
   */
  private LinuxContainerRuntime pickContainerRuntime(Container container)
      throws ContainerExecutionException {
    return pickContainerRuntime(container.getLaunchContext().getEnvironment());
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    // 选择对应运行试，委托执行容器准备操作
    LinuxContainerRuntime runtime = pickContainerRuntime(ctx.getContainer());
    runtime.prepareContainer(ctx);
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    // 选择对应运行试，委托执行容器启动操作
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.launchContainer(ctx);
  }

  @Override
  public void relaunchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    // 选择对应运行试，委托执行容器重新启动操作
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.relaunchContainer(ctx);
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    // 选择对应运行试，委托执行容器信号发送操作
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.signalContainer(ctx);
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    // 选择对应运行试，委托执行容器回收操作
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.reapContainer(ctx);
  }

  @Override
  public String[] getIpAndHost(Container container)
      throws ContainerExecutionException {
    // 选择对应运行试，委托获取容器IP和主机名
    LinuxContainerRuntime runtime = pickContainerRuntime(container);
    return runtime.getIpAndHost(container);
  }

  @Override
  public String getExposedPorts(Container container)
      throws ContainerExecutionException {
    // 选择对应运行试，委托获取容器暴露端口
    LinuxContainerRuntime runtime = pickContainerRuntime(container);
    return runtime.getExposedPorts(container);
  }

  /**
   * 判断运行时类型是否是自定义可插拔类型
   * @param runtimeType 运行时类型名称
   * @return 是自定义可插拔返回true，否则返回false
   */
  private boolean isPluggableRuntime(String runtimeType) {
    for (LinuxContainerRuntimeConstants.RuntimeType type :
        LinuxContainerRuntimeConstants.RuntimeType.values()) {
      if (type.name().equalsIgnoreCase(runtimeType)) {
        return false;
      }
    }
    return true;
  }

  /**
   * 根据配置创建自定义可插拔运行时实例
   * @param conf 配置对象
   * @param runtimeType 运行时类型名称
   * @return 创建好的运行时实例
   * @throws ContainerExecutionException 配置缺失或创建失败抛出异常
   */
  private LinuxContainerRuntime createPluggableRuntime(Configuration conf,
      String runtimeType) throws ContainerExecutionException {
    // 构造运行时类名配置key
    String confKey = String.format(
        YarnConfiguration.LINUX_CONTAINER_RUNTIME_CLASS_FMT, runtimeType);
    // 从配置加载运行时类
    Class<? extends LinuxContainerRuntime> clazz = conf.getClass(
        confKey, null, LinuxContainerRuntime.class);
    if (clazz == null) {
      throw new ContainerExecutionException("Invalid runtime set in "
          + YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES + " : "
          + runtimeType + " : Missing configuration " + confKey);
    }
    // 反射创建实例
    return ReflectionUtils.newInstance(clazz, conf);
  }

  /**
   * 判断运行时是否被允许启用
   * @param runtimeType 运行时类型名称
   * @return 允许返回true，否则返回false
   */
  @VisibleForTesting
  boolean isRuntimeAllowed(String runtimeType) {
    return runtimeType != null && allowedRuntimes.contains(
        runtimeType.toUpperCase());
  }

  @Override
  public IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    // 选择对应运行试，委托执行容器命令执行
    LinuxContainerRuntime runtime = pickContainerRuntime(container);
    return runtime.execContainer(ctx);
  }


  @Override
  public Map<String, LocalResource> getLocalResources(Container container)
      throws IOException {
    try {
      // 选择对应运行试，委托获取容器本地资源
      LinuxContainerRuntime runtime = pickContainerRuntime(container);
      return runtime.getLocalResources(container);
    } catch (ContainerExecutionException e) {
      throw new IOException(e);
    }
  }

  /**
   * 启动所有已允许的运行时
   */
  @Override
  public void start() {
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.JAVASANDBOX.name())) {
      javaSandboxLinuxContainerRuntime.start();
    }
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER.name())) {
      dockerLinuxContainerRuntime.start();
    }
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.RUNC.name())) {
      runcContainerRuntime.start();
    }
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT.name())) {
      defaultLinuxContainerRuntime.start();
    }

  }

  /**
   * 停止所有已允许的运行时
   */
  @Override
  public void stop() {
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.JAVASANDBOX.name())) {
      javaSandboxLinuxContainerRuntime.stop();
    }
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER.name())) {
      dockerLinuxContainerRuntime.stop();
    }
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.RUNC.name())) {
      runcContainerRuntime.stop();
    }
    if (isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT.name())) {
      defaultLinuxContainerRuntime.stop();
    }

  }

}