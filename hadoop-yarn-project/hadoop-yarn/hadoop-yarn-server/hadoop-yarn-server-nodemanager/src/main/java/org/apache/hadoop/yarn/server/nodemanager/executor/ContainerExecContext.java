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

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 封装容器启动/执行所需的全部上下文信息，供NodeManager容器执行器使用
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerExecContext {
  private final String user;
  private final String appId;
  private final Container container;
  private String command;
  private final LocalDirsHandlerService localDirsHandler;

  /**
   * ContainerExecContext的构造器，用于构建ContainerExecContext实例
   */
  public static final class Builder {
    private String user;
    private String appId;
    private Container container;
    private String command;
    private LocalDirsHandlerService localDirsHandler;

    /**
     * 构造空的Builder实例
     */
    public Builder() {
    }

    /**
     * 设置容器信息
     * @param c 容器对象
     * @return 当前Builder实例
     */
    public Builder setContainer(Container c) {
      this.container = c;
      return this;
    }

    /**
     * 设置提交容器的用户名
     * @param user 用户名
     * @return 当前Builder实例
     */
    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    /**
     * 设置应用ID
     * @param appId 应用标识
     * @return 当前Builder实例
     */
    public Builder setAppId(String appId) {
      this.appId = appId;
      return this;
    }

    /**
     * 构建ContainerExecContext实例
     * @return 构建完成的上下文对象
     */
    public ContainerExecContext build() {
      return new ContainerExecContext(this);
    }

    /**
     * 设置NodeManager本地目录处理器
     * @param ldhs 本地目录处理器服务
     * @return 当前Builder实例
     */
    public Builder setNMLocalPath(
        LocalDirsHandlerService ldhs) {
      this.localDirsHandler = ldhs;
      return this;
    }

    /**
     * 设置容器启动命令
     * @param command 启动shell命令
     * @return 当前Builder实例
     */
    public Builder setShell(String command) {
      this.command = command;
      return this;
    }
  }

  private ContainerExecContext(Builder builder) {
    this.user = builder.user;
    this.appId = builder.appId;
    this.container = builder.container;
    this.command = builder.command;
    this.localDirsHandler = builder.localDirsHandler;
  }

  /**
   * 获取提交容器的用户名
   * @return 用户名
   */
  public String getUser() {
    return this.user;
  }

  /**
   * 获取应用ID
   * @return 应用标识
   */
  public String getAppId() {
    return this.appId;
  }

  /**
   * 获取容器对象
   * @return 容器实例
   */
  public Container getContainer() {
    return this.container;
  }

  /**
   * 获取容器启动命令
   * @return 启动shell命令
   */
  public String getShell() {
    return this.command;
  }

  /**
   * 获取NodeManager本地目录处理器
   * @return 本地目录处理器服务实例
   */
  public LocalDirsHandlerService getLocalDirsHandlerService() {
    return this.localDirsHandler;
  }
}