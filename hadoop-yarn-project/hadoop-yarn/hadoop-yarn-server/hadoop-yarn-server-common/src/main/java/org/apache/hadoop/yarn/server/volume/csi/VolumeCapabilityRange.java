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
package org.apache.hadoop.yarn.server.volume.csi;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;

/**
 * YARN CSI存储卷能力范围类，存储卷资源请求中指定的容量范围，
 * 定义了存储卷期望的最小/最大容量边界。
 */
public final class VolumeCapabilityRange {

  private final long minCapacity;
  private final long maxCapacity;
  private final String unit;

  /**
   * 构造函数，由Builder调用创建实例。
   * @param minCapacity 最小容量
   * @param maxCapacity 最大容量
   * @param unit 容量单位
   */
  private VolumeCapabilityRange(long minCapacity,
      long maxCapacity, String unit) {
    this.minCapacity = minCapacity;
    this.maxCapacity = maxCapacity;
    this.unit = unit;
  }

  /**
   * 获取期望的最小存储容量。
   * @return 最小容量值
   */
  public long getMinCapacity() {
    return minCapacity;
  }

  /**
   * 获取期望的最大存储容量。
   * @return 最大容量值
   */
  public long getMaxCapacity() {
    return maxCapacity;
  }

  /**
   * 获取容量单位。
   * @return 容量单位字符串
   */
  public String getUnit() {
    return unit;
  }

  @Override
  public String toString() {
    return "MinCapability: " + minCapacity + unit
        + ", MaxCapability: " + maxCapacity + unit;
  }

  /**
   * 创建新的构建器实例。
   * @return VolumeCapabilityBuilder实例
   */
  public static VolumeCapabilityBuilder newBuilder() {
    return new VolumeCapabilityBuilder();
  }

  /**
   * 用于构建VolumeCapabilityRange实例的Builder类。
   */
  public static class VolumeCapabilityBuilder {
    // 默认无效值，表示该参数必须被显式设置
    private long minCap = -1L;
    private long maxCap = Long.MAX_VALUE;
    private String unit;

    /**
     * 设置最小容量。
     * @param minCapacity 最小容量值
     * @return 当前Builder实例
     */
    public VolumeCapabilityBuilder minCapacity(long minCapacity) {
      this.minCap = minCapacity;
      return this;
    }

    /**
     * 设置最大容量。
     * @param maxCapacity 最大容量值
     * @return 当前Builder实例
     */
    public VolumeCapabilityBuilder maxCapacity(long maxCapacity) {
      this.maxCap = maxCapacity;
      return this;
    }

    /**
     * 设置容量单位。
     * @param capacityUnit 容量单位字符串
     * @return 当前Builder实例
     */
    public VolumeCapabilityBuilder unit(String capacityUnit) {
      this.unit = capacityUnit;
      return this;
    }

    /**
     * 构建并验证VolumeCapabilityRange实例。
     * @return 构建完成的实例
     * @throws InvalidVolumeException 如果参数不合法抛出异常
     */
    public VolumeCapabilityRange build() throws InvalidVolumeException {
      VolumeCapabilityRange
          capability = new VolumeCapabilityRange(minCap, maxCap, unit);
      validateCapability(capability);
      return capability;
    }

    /**
     * 验证容量范围参数的合法性。
     * @param capability 待验证的容量范围实例
     * @throws InvalidVolumeException 验证失败抛出异常
     */
    private void validateCapability(VolumeCapabilityRange capability)
        throws InvalidVolumeException {
      // 验证最小容量不能为负
      if (capability.getMinCapacity() < 0) {
        throw new InvalidVolumeException("Invalid volume capability range,"
            + " minimal capability must not be less than 0. Capability: "
            + capability.toString());
      }
      // 验证容量单位不能为空
      if (Strings.isNullOrEmpty(capability.getUnit())) {
        throw new InvalidVolumeException("Invalid volume capability range,"
            + " capability unit is missing. Capability: "
            + capability.toString());
      }
    }
  }
}