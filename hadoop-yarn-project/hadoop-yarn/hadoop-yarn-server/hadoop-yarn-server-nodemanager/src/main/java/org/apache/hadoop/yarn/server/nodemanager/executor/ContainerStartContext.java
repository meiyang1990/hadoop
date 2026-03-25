// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 封装YARN NodeManager启动容器所需的全部上下文信息
 * 用于容器启动执行器传递容器启动参数，隔离参数构造与执行逻辑
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerStartContext {
  private final Container container;
  private final Map<Path, List<String>> localizedResources;
  private final Path nmPrivateContainerScriptPath;
  private final Path nmPrivateTokensPath;
  private final Path nmPrivateKeystorePath;
  private final Path nmPrivateTruststorePath;
  private final String user;
  private final String appId;
  private final Path containerWorkDir;
  private final Path csiVolumesRootDir;
  private final List<String> localDirs;
  private final List<String> logDirs;
  private final List<String> filecacheDirs;
  private final List<String> userLocalDirs;
  private final List<String> containerLocalDirs;
  private final List<String> containerLogDirs;
  private final List<String> userFilecacheDirs;
  private final List<String> applicationLocalDirs;

  /**
   * ContainerStartContext建造器，用于构建复杂的ContainerStartContext对象
   */
  public static final class Builder {
    private Container container;
    private Map<Path, List<String>> localizedResources;
    private Path nmPrivateContainerScriptPath;
    private Path nmPrivateTokensPath;
    private Path nmPrivateKeystorePath;
    private Path nmPrivateTruststorePath;
    private String user;
    private String appId;
    private Path containerWorkDir;
    private Path csiVolumesRoot;
    private List<String> localDirs;
    private List<String> logDirs;
    private List<String> filecacheDirs;
    private List<String> userLocalDirs;
    private List<String> containerLocalDirs;
    private List<String> containerLogDirs;
    private List<String> userFilecacheDirs;
    private List<String> applicationLocalDirs;

    /**
     * 构造空建造器实例
     */
    public Builder() {
    }

    /**
     * 设置待启动容器对象
     * @param container 待启动容器
     * @return 当前建造器实例
     */
    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    /**
     * 设置本地化资源映射，key为资源路径，value为资源链接名列表
     * @param localizedResources 本地化资源映射
     * @return 当前建造器实例
     */
    public Builder setLocalizedResources(Map<Path,
        List<String>> localizedResources) {
      this.localizedResources = localizedResources;
      return this;
    }

    /**
     * 设置NodeManager私有目录下容器启动脚本路径
     * @param nmPrivateContainerScriptPath 容器启动脚本路径
     * @return 当前建造器实例
     */
    public Builder setNmPrivateContainerScriptPath(
        Path nmPrivateContainerScriptPath) {
      this.nmPrivateContainerScriptPath = nmPrivateContainerScriptPath;
      return this;
    }

    /**
     * 设置NodeManager私有目录下令牌文件路径
     * @param nmPrivateTokensPath 令牌文件路径
     * @return 当前建造器实例
     */
    public Builder setNmPrivateTokensPath(Path nmPrivateTokensPath) {
      this.nmPrivateTokensPath = nmPrivateTokensPath;
      return this;
    }

    /**
     * 设置NodeManager私有目录下密钥库路径
     * @param nmPrivateKeystorePath 密钥库路径
     * @return 当前建造器实例
     */
    public Builder setNmPrivateKeystorePath(Path nmPrivateKeystorePath) {
      this.nmPrivateKeystorePath = nmPrivateKeystorePath;
      return this;
    }

    /**
     * 设置NodeManager私有目录下信任库路径
     * @param nmPrivateTruststorePath 信任库路径
     * @return 当前建造器实例
     */
    public Builder setNmPrivateTruststorePath(Path nmPrivateTruststorePath) {
      this.nmPrivateTruststorePath = nmPrivateTruststorePath;
      return this;
    }

    /**
     * 设置容器所属用户
     * @param user 用户名
     * @return 当前建造器实例
     */
    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    /**
     * 设置容器所属应用ID
     * @param appId 应用ID
     * @return 当前建造器实例
     */
    public Builder setAppId(String appId) {
      this.appId = appId;
      return this;
    }

    /**
     * 设置CSI卷根目录路径
     * @param csiVolumesRootDir CSI卷根目录路径
     * @return 当前建造器实例
     */
    public Builder setContainerCsiVolumesRootDir(Path csiVolumesRootDir) {
      this.csiVolumesRoot = csiVolumesRootDir;
      return this;
    }

    /**
     * 设置容器工作目录路径
     * @param containerWorkDir 容器工作目录路径
     * @return 当前建造器实例
     */
    public Builder setContainerWorkDir(Path containerWorkDir) {
      this.containerWorkDir = containerWorkDir;
      return this;
    }

    /**
     * 设置NodeManager本地目录列表
     * @param localDirs 本地目录列表
     * @return 当前建造器实例
     */
    public Builder setLocalDirs(List<String> localDirs) {
      this.localDirs = localDirs;
      return this;
    }

    /**
     * 设置日志目录列表
     * @param logDirs 日志目录列表
     * @return 当前建造器实例
     */
    public Builder setLogDirs(List<String> logDirs) {
      this.logDirs = logDirs;
      return this;
    }

    /**
     * 设置文件缓存目录列表
     * @param filecacheDirs 文件缓存目录列表
     * @return 当前建造器实例
     */
    public Builder setFilecacheDirs(List<String> filecacheDirs) {
      this.filecacheDirs = filecacheDirs;
      return this;
    }

    /**
     * 设置用户本地目录列表
     * @param userLocalDirs 用户本地目录列表
     * @return 当前建造器实例
     */
    public Builder setUserLocalDirs(List<String> userLocalDirs) {
      this.userLocalDirs = userLocalDirs;
      return this;
    }

    /**
     * 设置容器本地目录列表
     * @param containerLocalDirs 容器本地目录列表
     * @return 当前建造器实例
     */
    public Builder setContainerLocalDirs(List<String> containerLocalDirs) {
      this.containerLocalDirs = containerLocalDirs;
      return this;
    }

    /**
     * 设置容器日志目录列表
     * @param containerLogDirs 容器日志目录列表
     * @return 当前建造器实例
     */
    public Builder setContainerLogDirs(List<String> containerLogDirs) {
      this.containerLogDirs = containerLogDirs;
      return this;
    }

    /**
     * 设置用户文件缓存目录列表
     * @param userFilecacheDirs 用户文件缓存目录列表
     * @return 当前建造器实例
     */
    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setUserFilecacheDirs(List<String> userFilecacheDirs) {
      this.userFilecacheDirs = userFilecacheDirs;
      return this;
    }

    /**
     * 设置应用本地目录列表
     * @param applicationLocalDirs 应用本地目录列表
     * @return 当前建造器实例
     */
    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setApplicationLocalDirs(List<String> applicationLocalDirs) {
      this.applicationLocalDirs = applicationLocalDirs;
      return this;
    }

    /**
     * 构建ContainerStartContext实例
     * @return 构建完成的容器启动上下文对象
     */
    public ContainerStartContext build() {
      return new ContainerStartContext(this);
    }
  }

  private ContainerStartContext(Builder builder) {
    this.container = builder.container;
    this.localizedResources = builder.localizedResources;
    this.nmPrivateContainerScriptPath = builder.nmPrivateContainerScriptPath;
    this.nmPrivateTokensPath = builder.nmPrivateTokensPath;
    this.nmPrivateKeystorePath = builder.nmPrivateKeystorePath;
    this.nmPrivateTruststorePath = builder.nmPrivateTruststorePath;
    this.user = builder.user;
    this.appId = builder.appId;
    this.containerWorkDir = builder.containerWorkDir;
    this.localDirs = builder.localDirs;
    this.logDirs = builder.logDirs;
    this.filecacheDirs = builder.filecacheDirs;
    this.userLocalDirs = builder.userLocalDirs;
    this.containerLocalDirs = builder.containerLocalDirs;
    this.containerLogDirs = builder.containerLogDirs;
    this.userFilecacheDirs = builder.userFilecacheDirs;
    this.applicationLocalDirs = builder.applicationLocalDirs;
    this.csiVolumesRootDir = builder.csiVolumesRoot;
  }

  /**
   * 获取待启动容器对象
   * @return 待启动容器
   */
  public Container getContainer() {
    return this.container;
  }

  /**
   * 获取本地化资源映射
   * @return 不可修改的本地化资源映射
   */
  public Map<Path, List<String>> getLocalizedResources() {
    if (this.localizedResources != null) {
      return Collections.unmodifiableMap(this.localizedResources);
    } else {
      return null;
    }
  }

  /**
   * 获取NodeManager私有目录下容器启动脚本路径
   * @return 容器启动脚本路径
   */
  public Path getNmPrivateContainerScriptPath() {
    return this.nmPrivateContainerScriptPath;
  }

  /**
   * 获取NodeManager私有目录下令牌文件路径
   * @return 令牌文件路径
   */
  public Path getNmPrivateTokensPath() {
    return this.nmPrivateTokensPath;
  }

  /**
   * 获取NodeManager私有目录下密钥库路径
   * @return 密钥库路径
   */
  public Path getNmPrivateKeystorePath() {
    return this.nmPrivateKeystorePath;
  }

  /**
   * 获取NodeManager私有目录下信任库路径
   * @return 信任库路径
   */
  public Path getNmPrivateTruststorePath() {
    return this.nmPrivateTruststorePath;
  }

  /**
   * 获取容器所属用户名
   * @return 用户名
   */
  public String getUser() {
    return this.user;
  }

  /**
   * 获取容器所属应用ID
   * @return 应用ID字符串
   */
  public String getAppId() {
    return this.appId;
  }

  /**
   * 获取容器工作目录路径
   * @return 容器工作目录路径
   */
  public Path getContainerWorkDir() {
    return this.containerWorkDir;
  }

  /**
   * 获取NodeManager本地目录列表
   * @return 不可修改的本地目录列表
   */
  public List<String> getLocalDirs() {
    return Collections.unmodifiableList(this.localDirs);
  }

  /**
   * 获取日志目录列表
   * @return 不可修改的日志目录列表
   */
  public List<String> getLogDirs() {
    return Collections.unmodifiableList(this.logDirs);
  }

  /**
   * 获取文件缓存目录列表
   * @return 不可修改的文件缓存目录列表
   */
  public List<String> getFilecacheDirs() {
    return Collections.unmodifiableList(this.filecacheDirs);
  }

  /**
   * 获取用户本地目录列表
   * @return 不可修改的用户本地目录列表
   */
  public List<String> getUserLocalDirs() {
    return Collections.unmodifiableList(this.userLocalDirs);
  }

  /**
   * 获取容器本地目录列表
   * @return 不可修改的容器本地目录列表
   */
  public List<String> getContainerLocalDirs() {
    return Collections.unmodifiableList(this.containerLocalDirs);
  }

  /**
   * 获取容器日志目录列表
   * @return 不可修改的容器日志目录列表
   */
  public List<String> getContainerLogDirs() {
    return Collections.unmodifiableList(this
        .containerLogDirs);
  }

  /**
   * 获取用户文件缓存目录列表
   * @return 不可修改的用户文件缓存目录列表
   */
  public List<String> getUserFilecacheDirs() {
    return Collections.unmodifiableList(this.userFilecacheDirs);
  }

  /**
   * 获取应用本地目录列表
   * @return 不可修改的应用本地目录列表
   */
  public List<String> getApplicationLocalDirs() {
    return Collections.unmodifiableList(this.applicationLocalDirs);
  }

  /**
   * 获取CSI卷根目录路径
   * @return CSI卷根目录路径
   */
  public Path getCsiVolumesRootDir() {
    return this.csiVolumesRootDir;
  }
}