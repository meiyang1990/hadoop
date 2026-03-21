// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.util.Objects;

/**
 * 设备插件注册请求，封装设备插件向NodeManager注册时需要提交的信息
 * 用于YARN节点管理器对第三方设备插件的注册管理流程
 * */
public final class DeviceRegisterRequest {

  // 设备插件自身的版本号
  private final String pluginVersion;
  // 设备插件管理的资源名称（如GPU、FPGA等）
  private final String resourceName;

  private DeviceRegisterRequest(Builder builder) {
    this.resourceName = Objects.requireNonNull(builder.resourceName);
    this.pluginVersion = builder.pluginVersion;
  }

  /**
   * 获取该设备插件管理的资源名称
   * @return 资源名称
   */
  public String getResourceName() {
    return resourceName;
  }

  /**
   * 获取设备插件的版本号
   * @return 插件版本号
   */
  public String getPluginVersion() {
    return pluginVersion;
  }

  /**
   * DeviceRegisterRequest的Builder构造器类，用于安全构建请求对象
   * */
  public final static class Builder {
    private String pluginVersion;
    private String resourceName;

    private Builder() {}

    /**
     * 创建新的构建器实例
     * @return 构建器实例
     */
    public static Builder newInstance() {
      return new Builder();
    }

    /**
     * 构建DeviceRegisterRequest对象
     * @return 构建完成的注册请求对象
     */
    public DeviceRegisterRequest build() {
      return new DeviceRegisterRequest(this);
    }

    /**
     * 设置注册请求中的资源名称
     * @param resName 资源名称
     * @return 当前构建器实例
     */
    public Builder setResourceName(String resName) {
      this.resourceName = resName;
      return this;
    }

    /**
     * 设置注册请求中的插件版本号
     * @param plVersion 插件版本号
     * @return 当前构建器实例
     */
    public Builder setPluginVersion(String plVersion) {
      this.pluginVersion = plVersion;
      return this;
    }

  }
}