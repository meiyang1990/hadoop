// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 容器回收上下文，封装回收容器所需的全部参数信息。
 * 用于NodeManager清理终止容器时传递上下文参数。
 */
public final class ContainerReapContext {

  private final Container container;
  private final String user;

  /**
   * ContainerReapContext的构造器，采用Builder模式构造上下文对象。
   */
  public static final class Builder {
    private Container builderContainer;
    private String builderUser;

    public Builder() {
    }

    /**
     * 设置待回收的容器对象。
     *
     * @param container 待回收的容器
     * @return 当前Builder实例
     */
    public Builder setContainer(Container container) {
      this.builderContainer = container;
      return this;
    }

    /**
     * 设置容器对应用户。
     *
     * @param user 容器所属用户
     * @return 当前Builder实例
     */
    public Builder setUser(String user) {
      this.builderUser = user;
      return this;
    }

    /**
     * 构造最终的容器回收上下文对象。
     *
     * @return 构造完成的ContainerReapContext实例
     */
    public ContainerReapContext build() {
      return new ContainerReapContext(this);
    }
  }

  private ContainerReapContext(Builder builder) {
    this.container = builder.builderContainer;
    this.user = builder.builderUser;
  }

  /**
   * 获取待回收的容器对象。
   *
   * @return 待回收的容器
   */
  public Container getContainer() {
    return container;
  }

  /**
   * 获取容器所属用户。
   *
   * @return 容器所属用户名
   */
  public String getUser() {
    return user;
  }
}