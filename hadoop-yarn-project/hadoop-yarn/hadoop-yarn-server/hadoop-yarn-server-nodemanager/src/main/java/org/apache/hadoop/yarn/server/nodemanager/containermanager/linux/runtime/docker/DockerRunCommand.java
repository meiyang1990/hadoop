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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 构造Docker run命令的构建器类，用于生成YARN容器在Docker中运行所需的命令参数
 */
public class DockerRunCommand extends DockerCommand {
  private static final String RUN_COMMAND = "run";
  // 保存用户自定义环境变量，用于容器启动时注入
  private final Map<String, String> userEnv;

  /**
   * 构造Docker run命令，必填参数初始化
   * @param containerId YARN容器ID
   * @param user 容器运行用户
   * @param image 容器镜像名称
   */
  public DockerRunCommand(String containerId, String user, String image) {
    super(RUN_COMMAND);
    super.addCommandArguments("name", containerId);
    super.addCommandArguments("user", user);
    super.addCommandArguments("image", image);
    this.userEnv = new LinkedHashMap<String, String>();
  }

  /**
   * 容器退出后自动删除容器
   * @return 当前构建器实例
   */
  public DockerRunCommand removeContainerOnExit() {
    super.addCommandArguments("rm", "true");
    return this;
  }

  /**
   * 启动容器后进入后台分离模式运行
   * @return 当前构建器实例
   */
  public DockerRunCommand detachOnRun() {
    super.addCommandArguments("detach", "true");
    return this;
  }

  /**
   * 设置容器内工作目录
   * @param workdir 工作目录路径
   * @return 当前构建器实例
   */
  public DockerRunCommand setContainerWorkDir(String workdir) {
    super.addCommandArguments("workdir", workdir);
    return this;
  }

  /**
   * 设置容器网络类型
   * @param type 网络类型名称
   * @return 当前构建器实例
   */
  public DockerRunCommand setNetworkType(String type) {
    super.addCommandArguments("net", type);
    return this;
  }

  /**
   * 设置PID命名空间类型
   * @param type PID命名空间类型
   * @return 当前构建器实例
   */
  public DockerRunCommand setPidNamespace(String type) {
    super.addCommandArguments("pid", type);
    return this;
  }

  /**
   * 添加一个数据卷挂载
   * @param sourcePath 宿主机源路径
   * @param destinationPath 容器内目标路径
   * @param mode 挂载模式(ro/rw)
   * @return 当前构建器实例
   */
  public DockerRunCommand addMountLocation(String sourcePath, String
      destinationPath, String mode) {
    super.addCommandArguments("mounts", sourcePath + ":" +
        destinationPath + ":" + mode);
    return this;
  }

  /**
   * 添加可读写数据卷挂载
   * @param sourcePath 宿主机源路径
   * @param destinationPath 容器内目标路径
   * @return 当前构建器实例
   */
  public DockerRunCommand addReadWriteMountLocation(String sourcePath, String
      destinationPath) {
    return addMountLocation(sourcePath, destinationPath, "rw");
  }

  /**
   * 批量添加多个可读写数据卷挂载，源路径与容器内路径一致
   * @param paths 挂载路径列表
   * @return 当前构建器实例
   */
  public DockerRunCommand addAllReadWriteMountLocations(List<String> paths) {
    for (String dir: paths) {
      this.addReadWriteMountLocation(dir, dir);
    }
    return this;
  }

  /**
   * 添加只读数据卷挂载，可选择源路径不存在时自动创建
   * @param sourcePath 宿主机源路径
   * @param destinationPath 容器内目标路径
   * @param createSource 源路径不存在时是否创建
   * @return 当前构建器实例
   */
  public DockerRunCommand addReadOnlyMountLocation(String sourcePath, String
      destinationPath, boolean createSource) {
    boolean sourceExists = new File(sourcePath).exists();
    if (!sourceExists && !createSource) {
      return this;
    }
    return addReadOnlyMountLocation(sourcePath, destinationPath);
  }

  /**
   * 添加只读数据卷挂载
   * @param sourcePath 宿主机源路径
   * @param destinationPath 容器内目标路径
   * @return 当前构建器实例
   */
  public DockerRunCommand addReadOnlyMountLocation(String sourcePath, String
      destinationPath) {
    return addMountLocation(sourcePath, destinationPath, "ro");
  }

  /**
   * 批量添加多个只读数据卷挂载，源路径与容器内路径一致
   * @param paths 挂载路径列表
   * @return 当前构建器实例
   */
  public DockerRunCommand addAllReadOnlyMountLocations(List<String> paths) {
    for (String dir: paths) {
      this.addReadOnlyMountLocation(dir, dir);
    }
    return this;
  }

  /**
   * 添加tmpfs临时文件系统挂载
   * @param mount 挂载配置
   * @return 当前构建器实例
   */
  public DockerRunCommand addTmpfsMount(String mount) {
    super.addCommandArguments("tmpfs", mount);
    return this;
  }

  /**
   * 设置Docker卷驱动
   * @param volumeDriver 卷驱动名称
   * @return 当前构建器实例
   */
  public DockerRunCommand setVolumeDriver(String volumeDriver) {
    super.addCommandArguments("volume-driver", volumeDriver);
    return this;
  }

  /**
   * 设置cgroup父目录，用于YARN统一管理容器cgroup
   * @param parentPath cgroup父目录路径
   * @return 当前构建器实例
   */
  public DockerRunCommand setCGroupParent(String parentPath) {
    super.addCommandArguments("cgroup-parent", parentPath);
    return this;
  }

  /* Run a privileged container. Use with extreme care */
  /**
   * 启用特权容器模式，使用需谨慎
   * @return 当前构建器实例
   */
  public DockerRunCommand setPrivileged() {
    super.addCommandArguments("privileged", "true");
    return this;
  }

  /**
   * 设置容器可用的Linux能力(Capability)，默认先丢弃所有能力只保留指定的
   * @param capabilties 需要开启的能力集合
   * @return 当前构建器实例
   */
  public DockerRunCommand setCapabilities(Set<String> capabilties) {
    // 先丢弃所有能力
    super.addCommandArguments("cap-drop", "ALL");

    // 添加指定的能力
    for (String capability : capabilties) {
      super.addCommandArguments("cap-add", capability);
    }

    return this;
  }

  /**
   * 设置容器内主机名
   * @param hostname 主机名
   * @return 当前构建器实例
   */
  public DockerRunCommand setHostname(String hostname) {
    super.addCommandArguments("hostname", hostname);
    return this;
  }

  /**
   * 添加设备挂载到容器
   * @param sourceDevice 宿主机设备路径
   * @param destinationDevice 容器内设备路径
   * @return 当前构建器实例
   */
  public DockerRunCommand addDevice(String sourceDevice, String
      destinationDevice) {
    super.addCommandArguments("devices", sourceDevice + ":" +
        destinationDevice);
    return this;
  }

  /**
   * 启用分离模式运行
   * @return 当前构建器实例
   */
  public DockerRunCommand enableDetach() {
    super.addCommandArguments("detach", "true");
    return this;
  }

  /**
   * 禁用分离模式运行
   * @return 当前构建器实例
   */
  public DockerRunCommand disableDetach() {
    super.addCommandArguments("detach", "false");
    return this;
  }

  /* Ports mapping for bridge network, -p */
  /**
   * 添加端口映射配置，用于桥接网络模式
   * @param mapping 端口映射字符串
   * @return 当前构建器实例
   */
  public DockerRunCommand addPortsMapping(String mapping) {
    super.addCommandArguments("ports-mapping", mapping);
    return this;
  }

  /**
   * 指定Docker运行时类型
   * @param runtime 运行时名称
   * @return 当前构建器实例
   */
  public DockerRunCommand addRuntime(String runtime) {
    super.addCommandArguments("runtime", runtime);
    return this;
  }

  /**
   * 将指定用户组添加到容器内用户的组列表
   * @param groups 组名数组
   * @return 当前构建器实例
   */
  public DockerRunCommand groupAdd(String[] groups) {
    super.addCommandArguments("group-add", String.join(",", groups));
    return this;
  }

  /**
   * 设置覆盖容器镜像默认启动命令
   * @param overrideCommandWithArgs 启动命令和参数列表
   * @return 当前构建器实例
   */
  public DockerRunCommand setOverrideCommandWithArgs(
      List<String> overrideCommandWithArgs) {
    for(String override: overrideCommandWithArgs) {
      super.addCommandArguments("launch-command", override);
    }
    return this;
  }

  @Override
  public Map<String, List<String>> getDockerCommandWithArguments() {
    return super.getDockerCommandWithArguments();
  }

  /**
   * 设置是否禁用自定义启动命令，使用镜像原有入口点
   * @param toggle true表示禁用自定义命令
   * @return 当前构建器实例
   */
  public DockerRunCommand setOverrideDisabled(boolean toggle) {
    String value = Boolean.toString(toggle);
    super.addCommandArguments("use-entry-point", value);
    return this;
  }

  /**
   * 设置容器日志目录
   * @param logDir 日志目录路径
   * @return 当前构建器实例
   */
  public DockerRunCommand setLogDir(String logDir) {
    super.addCommandArguments("log-dir", logDir);
    return this;
  }

  /**
   * 设置是否启用服务运行模式
   * @param serviceMode true表示服务模式
   * @return 当前构建器实例
   */
  public DockerRunCommand setServiceMode(boolean serviceMode) {
    String value = Boolean.toString(serviceMode);
    super.addCommandArguments("service-mode", value);
    return this;
  }

  /**
   * 检查是否存在用户自定义环境变量
   *
   * @return true if user defined environment variables are not empty.
   */
  public boolean containsEnv() {
    if (userEnv.size() > 0) {
      return true;
    }
    return false;
  }

  /**
   * 获取用户自定义环境变量列表
   *
   * @return a map of user defined environment variables
   */
  public Map<String, String> getEnv() {
    return userEnv;
  }

  /**
   * 添加批量用户自定义环境变量
   *
   * @param environment A map of user defined environment variables
   */
  public final void addEnv(Map<String, String> environment) {
    userEnv.putAll(environment);
  }

  /**
   * 设置是否使用YARN管理的sysfs挂载
   * @param toggle true表示使用YARN管理的sysfs
   * @return 当前构建器实例
   */
  public DockerRunCommand setYarnSysFS(boolean toggle) {
    String value = Boolean.toString(toggle);
    super.addCommandArguments("use-yarn-sysfs", value);
    return this;
  }
}