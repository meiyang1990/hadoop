// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 存储cgroups挂载相关配置信息，支持v1和v2混合模式配置
 */
public class CGroupsMountConfig {
  private final boolean enableMount;
  private final String mountPath;

  // CGroups v2挂载路径仅在v1/v2混合模式下生效，此时v2和v1分别挂载
  private final String v2MountPath;

  /**
   * 从Yarn配置中加载cgroups挂载配置
   * @param conf Hadoop配置对象
   */
  public CGroupsMountConfig(Configuration conf) {
    this.enableMount = conf.getBoolean(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    this.mountPath = conf.get(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, null);
    this.v2MountPath = conf.get(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_V2_MOUNT_PATH, mountPath);
  }

  /**
   * 检查并确保cgroups挂载路径已配置，未配置则抛出异常
   * @return 检查通过返回true
   * @throws ResourceHandlerException 挂载路径未配置时抛出异常
   */
  public boolean ensureMountPathIsDefined() throws ResourceHandlerException {
    if (mountPath == null) {
      throw new ResourceHandlerException(
          String.format("Cgroups mount path not specified in %s.",
              YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH));
    }
    return true;
  }

  /**
   * 判断挂载路径是否已配置
   * @return true表示已配置，false表示未配置
   */
  public boolean isMountPathDefined() {
    return mountPath != null;
  }

  /**
   * 获取是否启用自动挂载cgroups
   * @return true表示启用自动挂载
   */
  public boolean isMountEnabled() {
    return enableMount;
  }

  /**
   * 判断是否关闭自动挂载但已手动配置了挂载路径
   * @return true表示关闭自动挂载且配置了手动挂载路径
   */
  public boolean mountDisabledButMountPathDefined() {
    return !enableMount && mountPath != null;
  }

  /**
   * 判断是否启用自动挂载且已配置挂载路径
   * @return true表示启用自动挂载且配置了挂载路径
   */
  public boolean mountEnabledAndMountPathDefined() {
    return enableMount && mountPath != null;
  }

  /**
   * 获取cgroups v1挂载路径
   * @return v1挂载路径
   */
  public String getMountPath() {
    return mountPath;
  }

  /**
   * 获取cgroups v2挂载路径
   * @return v2挂载路径，混合模式下使用
   */
  public String getV2MountPath() {
    return v2MountPath;
  }

  @Override
  public String toString() {
    return "CGroupsMountConfig{" +
        "enableMount=" + enableMount +
        ", mountPath='" + mountPath +
        ", v2MountPath='" + v2MountPath + '\'' +
        '}';
  }
}