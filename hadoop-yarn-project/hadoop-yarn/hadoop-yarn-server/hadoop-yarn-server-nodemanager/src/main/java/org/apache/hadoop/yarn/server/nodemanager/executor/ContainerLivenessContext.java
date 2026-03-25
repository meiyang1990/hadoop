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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 封装容器存活检查所需的上下文信息，为NodeManager端容器存活检测使用
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerLivenessContext {
  private final Container container;
  private final String user;
  private final String pid;

  /**
   * ContainerLivenessContext构建器，用于构建上下文实例
   */
  public static final class Builder {
    private Container container;
    private String user;
    private String pid;

    /**
     * 初始化空构建器
     */
    public Builder() {
    }

    /**
     * 设置待检查的容器对象
     * @param container 容器实例
     * @return 构建器自身
     */
    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    /**
     * 设置容器所属用户
     * @param user 用户名
     * @return 构建器自身
     */
    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    /**
     * 设置容器进程ID
     * @param pid 进程ID字符串
     * @return 构建器自身
     */
    public Builder setPid(String pid) {
      this.pid = pid;
      return this;
    }

    /**
     * 构建ContainerLivenessContext实例
     * @return 构建完成的上下文实例
     */
    public ContainerLivenessContext build() {
      return new ContainerLivenessContext(this);
    }
  }

  /**
   * 私有构造方法，通过构建器创建实例
   * @param builder 构建器
   */
  private ContainerLivenessContext(Builder builder) {
    this.container = builder.container;
    this.user = builder.user;
    this.pid = builder.pid;
  }

  /**
   * 获取待检查的容器实例
   * @return 容器对象
   */
  public Container getContainer() {
    return this.container;
  }

  /**
   * 获取容器所属用户名
   * @return 用户名
   */
  public String getUser() {
    return this.user;
  }

  /**
   * 获取容器进程ID
   * @return 进程ID字符串
   */
  public String getPid() {
    return this.pid;
  }
}