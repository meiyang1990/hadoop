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
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;

import java.net.InetSocketAddress;

/**
 * 封装启动本地化器所需的全部上下文信息，供NodeManager执行资源本地化时使用
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class LocalizerStartContext {
  private final Path nmPrivateContainerTokens;
  private final InetSocketAddress nmAddr;
  private final String user;
  private final String appId;
  private final String locId;
  private final LocalDirsHandlerService dirsHandler;

  /**
   * LocalizerStartContext的构建器，用于构造不可变的上下文实例
   */
  public static final class Builder {
    private Path nmPrivateContainerTokens;
    private InetSocketAddress nmAddr;
    private String user;
    private String appId;
    private String locId;
    private LocalDirsHandlerService dirsHandler;

    public Builder() {
    }

    /**
     * 设置NodeNode私有目录下容器令牌文件路径
     * @param nmPrivateContainerTokens 容器令牌文件路径
     * @return 当前构建器实例
     */
    public Builder setNmPrivateContainerTokens(Path nmPrivateContainerTokens) {
      this.nmPrivateContainerTokens = nmPrivateContainerTokens;
      return this;
    }

    /**
     * 设置NodeManager服务地址
     * @param nmAddr NodeManager地址
     * @return 当前构建器实例
     */
    public Builder setNmAddr(InetSocketAddress nmAddr) {
      this.nmAddr = nmAddr;
      return this;
    }

    /**
     * 设置应用提交用户
     * @param user 用户名
     * @return 当前构建器实例
     */
    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    /**
     * 设置应用ID
     * @param appId 应用ID
     * @return 当前构建器实例
     */
    public Builder setAppId(String appId) {
      this.appId = appId;
      return this;
    }

    /**
     * 设置本地化器ID
     * @param locId 本地化器ID
     * @return 当前构建器实例
     */
    public Builder setLocId(String locId) {
      this.locId = locId;
      return this;
    }

    /**
     * 设置本地目录处理器服务实例
     * @param dirsHandler 本地目录处理器
     * @return 当前构建器实例
     */
    public Builder setDirsHandler(LocalDirsHandlerService dirsHandler) {
      this.dirsHandler = dirsHandler;
      return this;
    }

    /**
     * 构造不可变的LocalizerStartContext实例
     * @return 构造完成的上下文实例
     */
    public LocalizerStartContext build() {
      return new LocalizerStartContext(this);
    }
  }

  private LocalizerStartContext(Builder builder) {
    this.nmPrivateContainerTokens = builder.nmPrivateContainerTokens;
    this.nmAddr = builder.nmAddr;
    this.user = builder.user;
    this.appId = builder.appId;
    this.locId = builder.locId;
    this.dirsHandler = builder.dirsHandler;
  }

  /**
   * 获取NodeManager私有目录下容器令牌文件路径
   * @return 容器令牌文件路径
   */
  public Path getNmPrivateContainerTokens() {
    return this.nmPrivateContainerTokens;
  }

  /**
   * 获取NodeManager服务地址
   * @return NodeManager地址
   */
  public InetSocketAddress getNmAddr() {
    return this.nmAddr;
  }

  /**
   * 获取应用提交用户名
   * @return 用户名
   */
  public String getUser() {
    return this.user;
  }

  /**
   * 获取应用ID
   * @return 应用ID
   */
  public String getAppId() {
    return this.appId;
  }

  /**
   * 获取本地化器ID
   * @return 本地化器ID
   */
  public String getLocId() {
    return this.locId;
  }

  /**
   * 获取本地目录处理器服务实例
   * @return 本地目录处理器
   */
  public LocalDirsHandlerService getDirsHandler() {
    return this.dirsHandler;
  }
}