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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 容器信号上下文，封装发送容器信号所需的全部信息。
 * 用于NodeManager中容器执行器模块，传递信号发送请求参数。
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerSignalContext {
  private final Container container;
  private final String user;
  private final String pid;
  private final Signal signal;

  /**
   * ContainerSignalContext构建器，使用Builder模式构造上下文对象。
   */
  public static final class Builder {
    private Container container;
    private String user;
    private String pid;
    private Signal signal;

    public Builder() {
    }

    /**
     * 设置目标容器。
     * @param container 目标容器
     * @return 当前构建器实例
     */
    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    /**
     * 设置容器对应用户。
     * @param user 容器运行用户
     * @return 当前构建器实例
     */
    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    /**
     * 设置容器进程ID。
     * @param pid 目标进程ID
     * @return 当前构建器实例
     */
    public Builder setPid(String pid) {
      this.pid = pid;
      return this;
    }

    /**
     * 设置要发送的信号。
     * @param signal 信号类型
     * @return 当前构建器实例
     */
    public Builder setSignal(Signal signal) {
      this.signal = signal;
      return this;
    }

    /**
     * 构建ContainerSignalContext实例。
     * @return 构造完成的容器信号上下文
     */
    public ContainerSignalContext build() {
      return new ContainerSignalContext(this);
    }
  }

  private ContainerSignalContext(Builder builder) {
    this.container = builder.container;
    this.user = builder.user;
    this.pid = builder.pid;
    this.signal = builder.signal;
  }

  /**
   * 获取目标容器实例。
   * @return 目标容器
   */
  public Container getContainer() {
    return this.container;
  }

  /**
   * 获取容器运行用户。
   * @return 运行用户
   */
  public String getUser() {
    return this.user;
  }

  /**
   * 获取目标进程ID。
   * @return 进程ID字符串
   */
  public String getPid() {
    return this.pid;
  }

  /**
   * 获取要发送的信号。
   * @return 信号类型
   */
  public Signal getSignal() {
    return this.signal;
  }

  /**
   * 判断是否指向同一个信号发送请求，基于PID、信号、容器、用户四个字段比较。
   * @param obj 待比较对象
   * @return 若为同一个信号请求返回true，否则返回false
   */
  @Override
  public boolean equals(Object obj) {
    // 类型检查
    if (obj instanceof ContainerSignalContext) {
      ContainerSignalContext other = (ContainerSignalContext)obj;
      // 比较PID字段
      boolean ret =
          (other.getPid() == null && getPid() == null) ||
              (other.getPid() != null && getPid() != null &&
                  other.getPid().equals(getPid()));
      // 继续比较信号字段
      ret = ret &&
          (other.getSignal() == null && getSignal() == null) ||
          (other.getSignal() != null && getSignal() != null &&
              other.getSignal().equals(getSignal()));
      // 继续比较容器字段
      ret = ret &&
          (other.getContainer() == null && getContainer() == null) ||
          (other.getContainer() != null && getContainer() != null &&
              other.getContainer().equals(getContainer()));
      // 继续比较用户字段
      ret = ret &&
          (other.getUser() == null && getUser() == null) ||
          (other.getUser() != null && getUser() != null &&
              other.getUser().equals(getUser()));
      return ret;
    }
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    // 使用HashCodeBuilder按四个字段计算哈希值
    return new HashCodeBuilder().
        append(getPid()).
        append(getSignal()).
        append(getContainer()).
        append(getUser()).
        toHashCode();
  }
}