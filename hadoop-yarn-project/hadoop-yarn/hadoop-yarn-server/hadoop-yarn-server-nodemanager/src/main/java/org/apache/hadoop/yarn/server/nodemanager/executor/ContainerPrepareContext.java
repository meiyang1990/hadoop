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
 * 容器准备上下文，封装容器启动准备阶段所需的全部信息，传递给容器准备执行器处理
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerPrepareContext {
  private final Container container;
  private final Map<Path, List<String>> localizedResources;
  private final String user;
  private final List<String> containerLocalDirs;
  private final List<String> commands;

  /**
   * ContainerPrepareContext 的 Builder 构造器，支持链式构建上下文对象
   */
  public static final class Builder {
    private Container container;
    private Map<Path, List<String>> localizedResources;
    private String user;
    private List<String> containerLocalDirs;
    private List<String> commands;

    public Builder() {
    }

    /**
     * 设置待准备的容器对象
     * @param container 容器实例
     * @return 当前Builder实例
     */
    public ContainerPrepareContext.Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    /**
     * 设置已经本地化完成的资源映射表
     * @param localizedResources 本地化资源路径和对应的权限列表映射
     * @return 当前Builder实例
     */
    public ContainerPrepareContext.Builder setLocalizedResources(Map<Path,
        List<String>> localizedResources) {
      this.localizedResources = localizedResources;
      return this;
    }

    /**
     * 设置提交容器的用户名
     * @param user 用户名
     * @return 当前Builder实例
     */
    public ContainerPrepareContext.Builder setUser(String user) {
      this.user = user;
      return this;
    }

    /**
     * 设置容器使用的本地目录列表
     * @param containerLocalDirs 容器本地目录列表
     * @return 当前Builder实例
     */
    public ContainerPrepareContext.Builder setContainerLocalDirs(
        List<String> containerLocalDirs) {
      this.containerLocalDirs = containerLocalDirs;
      return this;
    }

    /**
     * 构建ContainerPrepareContext实例
     * @return 构建完成的容器准备上下文对象
     */
    public ContainerPrepareContext build() {
      return new ContainerPrepareContext(this);
    }

    /**
     * 设置容器启动命令列表
     * @param commands 容器启动命令列表
     * @return 当前Builder实例
     */
    public ContainerPrepareContext.Builder setCommands(List<String> commands) {
      this.commands = commands;
      return this;
    }
  }

  private ContainerPrepareContext(ContainerPrepareContext.Builder builder) {
    this.container = builder.container;
    this.localizedResources = builder.localizedResources;
    this.user = builder.user;
    this.containerLocalDirs = builder.containerLocalDirs;
    this.commands = builder.commands;
  }

  /**
   * 获取待准备的容器对象
   * @return 容器实例
   */
  public Container getContainer() {
    return this.container;
  }

  /**
   * 获取已本地化资源的不可修改映射
   * @return 本地化资源映射，不存在则返回null
   */
  public Map<Path, List<String>> getLocalizedResources() {
    if (this.localizedResources != null) {
      return Collections.unmodifiableMap(this.localizedResources);
    } else {
      return null;
    }
  }

  /**
   * 获取提交容器的用户名
   * @return 用户名
   */
  public String getUser() {
    return this.user;
  }

  /**
   * 获取容器本地目录的不可修改列表
   * @return 容器本地目录列表
   */
  public List<String> getContainerLocalDirs() {
    return Collections.unmodifiableList(this.containerLocalDirs);
  }

  /**
   * 获取容器启动命令列表
   * @return 容器启动命令列表
   */
  public List<String> getCommands(){
    return this.commands;
  }
}