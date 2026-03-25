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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 容器重获取上下文，封装NodeManager重启后重新获取已有容器所需的全部信息
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerReacquisitionContext {
  // 待重获取的容器对象
  private final Container container;
  // 容器所属用户
  private final String user;
  // 容器ID
  private final ContainerId containerId;

  /**
   * ContainerReacquisitionContext构建器，支持链式构造上下文对象
   */
  public static final class Builder {
    private Container container;
    private String user;
    private ContainerId containerId;

    public Builder() {
    }

    /**
     * 设置待重获取的容器
     * @param container 容器对象
     * @return 当前构建器实例
     */
    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    /**
     * 设置容器所属用户
     * @param user 用户名
     * @return 当前构建器实例
     */
    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    /**
     * 设置容器ID
     * @param containerId 容器ID
     * @return 当前构建器实例
     */
    public Builder setContainerId(ContainerId containerId) {
      this.containerId = containerId;
      return this;
    }

    /**
     * 构造ContainerReacquisitionContext实例
     * @return 构建完成的上下文对象
     */
    public ContainerReacquisitionContext build() {
      return new ContainerReacquisitionContext(this);
    }
  }

  private ContainerReacquisitionContext(Builder builder) {
    this.container = builder.container;
    this.user = builder.user;
    this.containerId = builder.containerId;
  }

  /**
   * 获取待重获取的容器对象
   * @return 容器对象
   */
  public Container getContainer() {
    return this.container;
  }

  /**
   * 获取容器所属用户
   * @return 用户名
   */
  public String getUser() {
    return this.user;
  }

  /**
   * 获取容器ID
   * @return 容器ID
   */
  public ContainerId getContainerId() {
    return this.containerId;
  }
}