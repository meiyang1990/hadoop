// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import java.io.Serializable;

import org.apache.hadoop.util.Preconditions;

/**
 * 表示节点上一块FPGA设备的实体类，存储FPGA设备的基本信息和当前加载的比特流信息
 */
public class FpgaDevice implements Serializable {
  private static final long serialVersionUID = -4678487141824092751L;
  private final String type;
  private final int major;
  private final int minor;

  // FPGA设备别名，Intel平台使用acl0~acl31作为别名
  private final String aliasDevName;

  // 当前加载的IP文件标识符，例如矩阵乘法，可动态变更
  private String IPID;
  // 上传的aocx比特流文件的SHA-256哈希值，可动态变更
  private String aocxHash;

  // 缓存的哈希值，用于提升hashCode计算性能
  private Integer hashCode;

  public String getType() {
    return type;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public String getIPID() {
    return IPID;
  }

  public String getAocxHash() {
    return aocxHash;
  }

  public void setAocxHash(String hash) {
    this.aocxHash = hash;
  }

  public void setIPID(String IPID) {
    this.IPID = IPID;
  }

  public String getAliasDevName() {
    return aliasDevName;
  }

  /**
   * 构造FPGA设备对象，校验必填参数非空
   * @param type FPGA设备类型
   * @param major 设备驱动主设备号
   * @param minor 设备驱动次设备号
   * @param aliasDevName 设备别名
   */
  public FpgaDevice(String type, int major, int minor, String aliasDevName) {
    this.type = Preconditions.checkNotNull(type, "type must not be null");
    this.major = major;
    this.minor = minor;
    this.aliasDevName = Preconditions.checkNotNull(aliasDevName,
        "aliasDevName must not be null");
  }

  @Override
  public boolean equals(Object obj) {
    // 同一对象直接返回相等
    if (this == obj) {
      return true;
    }
    // 比较对象为null直接不相等
    if (obj == null) {
      return false;
    }
    // 类型不同直接不相等
    if (getClass() != obj.getClass()) {
      return false;
    }
    FpgaDevice other = (FpgaDevice) obj;
    // 比较设备别名
    if (aliasDevName == null) {
      if (other.aliasDevName != null) {
        return false;
      }
    } else if (!aliasDevName.equals(other.aliasDevName)) {
      return false;
    }
    // 比较主设备号
    if (major != other.major) {
      return false;
    }
    // 比较次设备号
    if (minor != other.minor) {
      return false;
    }
    // 比较设备类型
    if (type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!type.equals(other.type)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    // 延迟计算，缓存结果提升性能
    if (hashCode == null) {
      final int prime = 31;
      int result = 1;

      result = prime * result + major;
      result = prime * result + type.hashCode();
      result = prime * result + minor;
      result = prime * result + aliasDevName.hashCode();

      hashCode = result;
    }

    return hashCode;
  }

  @Override
  public String toString() {
    return "FPGA Device:(Type: " + this.type + ", Major: " + this.major
        + ", Minor: " + this.minor + ", IPID: " + this.IPID + ", Hash: "
        + this.aocxHash + ")";
  }

}