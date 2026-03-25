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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi;

import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeImpl;
import org.apache.hadoop.yarn.server.volume.csi.VolumeCapabilityRange;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;

import java.util.Optional;
import java.util.UUID;

/**
 * YARN CSI卷构建工具类，使用Builder模式构造Volume对象，支持链式调用和默认值填充
 */
public final class VolumeBuilder {

  private String id;
  private String name;
  private Long min;
  private Long max;
  private String unit;
  private String driver;
  private String mount;

  private VolumeBuilder() {
    // 隐藏构造函数，强制使用newBuilder创建实例
  }

  /**
   * 创建新的VolumeBuilder实例
   * @return VolumeBuilder实例
   */
  public static VolumeBuilder newBuilder() {
    return new VolumeBuilder();
  }

  /**
   * 设置卷ID
   * @param volumeId 卷ID
   * @return 当前Builder实例
   */
  public VolumeBuilder volumeId(String volumeId) {
    this.id = volumeId;
    return this;
  }

  /**
   * 设置卷名称
   * @param volumeName 卷名称
   * @return 当前Builder实例
   */
  public VolumeBuilder volumeName(String volumeName) {
    this.name = volumeName;
    return this;
  }

  /**
   * 设置最小容量能力
   * @param minCapability 最小容量
   * @return 当前Builder实例
   */
  public VolumeBuilder minCapability(long minCapability) {
    this.min = Long.valueOf(minCapability);
    return this;
  }

  /**
   * 设置最大容量能力
   * @param maxCapability 最大容量
   * @return 当前Builder实例
   */
  public VolumeBuilder maxCapability(long maxCapability) {
    this.max = Long.valueOf(maxCapability);
    return this;
  }

  /**
   * 设置容量单位
   * @param capUnit 容量单位
   * @return 当前Builder实例
   */
  public VolumeBuilder unit(String capUnit) {
    this.unit = capUnit;
    return this;
  }

  /**
   * 设置CSI驱动名称
   * @param driverName CSI驱动名称
   * @return 当前Builder实例
   */
  public VolumeBuilder driverName(String driverName) {
    this.driver = driverName;
    return this;
  }

  /**
   * 设置挂载点路径
   * @param mountPoint 挂载点路径
   * @return 当前Builder实例
   */
  public VolumeBuilder mountPoint(String mountPoint) {
    this.mount = mountPoint;
    return this;
  }

  /**
   * 构建Volume对象，未设置的属性使用默认值填充
   * @return 构造完成的Volume对象
   * @throws InvalidVolumeException 当卷信息无效时抛出异常
   */
  public Volume build() throws InvalidVolumeException {
    // 生成卷ID，未指定则使用随机UUID
    VolumeId vid = new VolumeId(
        Optional.ofNullable(id)
            .orElse(UUID.randomUUID().toString()));

    // 构建容量范围，未指定的使用默认值：最小0、最大Long最大值、单位Gi
    VolumeCapabilityRange volumeCap = VolumeCapabilityRange.newBuilder()
        .minCapacity(Optional.ofNullable(min).orElse(0L))
        .maxCapacity(Optional.ofNullable(max).orElse(Long.MAX_VALUE))
        .unit(Optional.ofNullable(unit).orElse("Gi"))
        .build();

    // 构建卷元数据，未指定的使用默认值：驱动test-driver、挂载点/mnt/data
    VolumeMetaData meta = VolumeMetaData.newBuilder()
        .capability(volumeCap)
        .driverName(Optional.ofNullable(driver).orElse("test-driver"))
        .mountPoint(Optional.ofNullable(mount).orElse("/mnt/data"))
        .volumeName(name)
        .volumeId(vid)
        .build();
    return new VolumeImpl(meta);
  }
}