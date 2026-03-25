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

package org.apache.hadoop.yarn.server.records;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN服务组件状态存储的版本信息载体，用于RMState、NMState等状态数据
 * 包含主版本号和次版本号两个部分：
 * 主版本号变更代表不兼容的状态结构变更，次版本号变更代表兼容的结构变更
 */
@LimitedPrivate({"YARN", "MapReduce"})
@Unstable
public abstract class Version {

  /**
   * 创建Version实例，设置指定的主版本号和次版本号
   * @param majorVersion 主版本号
   * @param minorVersion 次版本号
   * @return 初始化完成的Version实例
   */
  public static Version newInstance(int majorVersion, int minorVersion) {
    Version version = Records.newRecord(Version.class);
    version.setMajorVersion(majorVersion);
    version.setMinorVersion(minorVersion);
    return version;
  }

  public abstract int getMajorVersion();

  public abstract void setMajorVersion(int majorVersion);

  public abstract int getMinorVersion();

  public abstract void setMinorVersion(int minorVersion);

  @Override
  public String toString() {
    return getMajorVersion() + "." + getMinorVersion();
  }

  /**
   * 检查当前版本是否与目标版本兼容
   * @param version 待检查的目标版本
   * @return 主版本号相同则兼容，返回true；否则返回false
   */
  public boolean isCompatibleTo(Version version) {
    return getMajorVersion() == version.getMajorVersion();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getMajorVersion();
    result = prime * result + getMinorVersion();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Version other = (Version) obj;
    if (this.getMajorVersion() == other.getMajorVersion()
        && this.getMinorVersion() == other.getMinorVersion()) {
      return true;
    } else {
      return false;
    }
  }
}