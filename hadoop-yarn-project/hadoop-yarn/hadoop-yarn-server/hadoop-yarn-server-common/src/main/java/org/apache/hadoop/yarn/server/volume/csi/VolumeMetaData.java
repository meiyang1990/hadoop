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
import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;

import java.util.ArrayList;
import java.util.List;

/**
 * 存储兼容CSI标准的存储卷的所有元数据信息，用于YARN CSI存储卷管理
 */
public class VolumeMetaData {

  private VolumeId volumeId;
  private String volumeName;
  private VolumeCapabilityRange volumeCapabilityRange;
  private String driverName;
  private String mountPoint;

  private void setVolumeId(VolumeId volumeId) {
    this.volumeId = volumeId;
  }

  private void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  private void setVolumeCapabilityRange(VolumeCapabilityRange capability) {
    this.volumeCapabilityRange = capability;
  }

  private void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  private void setMountPoint(String mountPoint) {
    this.mountPoint = mountPoint;
  }

  /**
   * 判断存储卷是否已经完成预配置
   * @return 已预配置返回true，否则false
   */
  public boolean isProvisionedVolume() {
    return this.volumeId != null;
  }

  public VolumeId getVolumeId() {
    return volumeId;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public VolumeCapabilityRange getVolumeCapabilityRange() {
    return volumeCapabilityRange;
  }

  public String getDriverName() {
    return driverName;
  }

  public String getMountPoint() {
    return mountPoint;
  }

  /**
   * 创建VolumeSpecBuilder构建器实例
   * @return 新的构建器实例
   */
  public static VolumeSpecBuilder newBuilder() {
    return new VolumeSpecBuilder();
  }

  /**
   * 从YARN资源信息中解析出CSI存储卷元数据列表
   * @param resourceInfo YARN资源信息对象
   * @return 解析得到的存储卷元数据列表
   * @throws InvalidVolumeException 当存储卷信息无效时抛出异常
   */
  public static List<VolumeMetaData> fromResource(
      ResourceInformation resourceInfo) throws InvalidVolumeException {
    List<VolumeMetaData> volumeMetaData = new ArrayList<>();
    if (resourceInfo != null) {
      // 验证资源是否带有CSI存储卷标签
      if (resourceInfo.getTags() != null && resourceInfo.getTags()
          .contains(CsiConstants.CSI_VOLUME_RESOURCE_TAG)) {
        VolumeSpecBuilder builder = VolumeMetaData.newBuilder();
        // 解析存储卷ID
        if (resourceInfo.getAttributes()
            .containsKey(CsiConstants.CSI_VOLUME_ID)) {
          String id = resourceInfo.getAttributes()
              .get(CsiConstants.CSI_VOLUME_ID);
          builder.volumeId(new VolumeId(id));
        }
        // 解析存储卷名称
        if (resourceInfo.getAttributes()
            .containsKey(CsiConstants.CSI_VOLUME_NAME)) {
          builder.volumeName(resourceInfo.getAttributes()
              .get(CsiConstants.CSI_VOLUME_NAME));
        }
        // 解析CSI驱动名称
        if (resourceInfo.getAttributes()
            .containsKey(CsiConstants.CSI_DRIVER_NAME)) {
          builder.driverName(resourceInfo.getAttributes()
              .get(CsiConstants.CSI_DRIVER_NAME));
        }
        // 解析挂载路径
        if (resourceInfo.getAttributes()
            .containsKey(CsiConstants.CSI_VOLUME_MOUNT)) {
          builder.mountPoint(resourceInfo.getAttributes()
              .get(CsiConstants.CSI_VOLUME_MOUNT));
        }
        // 基于资源信息构建存储卷容量范围
        VolumeCapabilityRange volumeCapabilityRange =
            VolumeCapabilityRange.newBuilder()
                .minCapacity(resourceInfo.getValue())
                .unit(resourceInfo.getUnits())
                .build();
        builder.capability(volumeCapabilityRange);
        volumeMetaData.add(builder.build());
      }
    }
    return volumeMetaData;
  }

  @Override
  public String toString() {
    JsonObject json = new JsonObject();
    if (!Strings.isNullOrEmpty(volumeName)) {
      json.addProperty(CsiConstants.CSI_VOLUME_NAME, volumeName);
    }
    if (volumeId != null) {
      json.addProperty(CsiConstants.CSI_VOLUME_ID, volumeId.toString());
    }
    if (volumeCapabilityRange != null) {
      json.addProperty(CsiConstants.CSI_VOLUME_CAPABILITY,
          volumeCapabilityRange.toString());
    }
    if (!Strings.isNullOrEmpty(driverName)) {
      json.addProperty(CsiConstants.CSI_DRIVER_NAME, driverName);
    }
    if (!Strings.isNullOrEmpty(mountPoint)) {
      json.addProperty(CsiConstants.CSI_VOLUME_MOUNT, mountPoint);
    }
    return json.toString();
  }

  /**
   * 用于构建VolumeMetaData实例的Builder模式构建器
   */
  public static class VolumeSpecBuilder {
    // @CreateVolumeRequest
    // The suggested name for the storage space.
    private VolumeId volumeId;
    private String volumeName;
    private VolumeCapabilityRange volumeCapabilityRange;
    private String driverName;
    private String mountPoint;

    public VolumeSpecBuilder volumeId(VolumeId volumeId) {
      this.volumeId = volumeId;
      return this;
    }

    public VolumeSpecBuilder volumeName(String name) {
      this.volumeName = name;
      return this;
    }

    public VolumeSpecBuilder driverName(String driverName) {
      this.driverName = driverName;
      return this;
    }

    public VolumeSpecBuilder mountPoint(String mountPoint) {
      this.mountPoint = mountPoint;
      return this;
    }

    public VolumeSpecBuilder capability(VolumeCapabilityRange capability) {
      this.volumeCapabilityRange = capability;
      return this;
    }

    /**
     * 构建并验证VolumeMetaData实例
     * @return 构建完成的VolumeMetaData实例
     * @throws InvalidVolumeException 当元数据不满足校验规则时抛出异常
     */
    public VolumeMetaData build() throws InvalidVolumeException {
      VolumeMetaData spec = new VolumeMetaData();
      spec.setVolumeId(volumeId);
      spec.setVolumeName(volumeName);
      spec.setVolumeCapabilityRange(volumeCapabilityRange);
      spec.setDriverName(driverName);
      spec.setMountPoint(mountPoint);
      validate(spec);
      return spec;
    }

    /**
     * 校验VolumeMetaData实例的必填字段是否完整
     * @param spec 待校验的VolumeMetaData实例
     * @throws InvalidVolumeException 当必填字段缺失时抛出异常
     */
    private void validate(VolumeMetaData spec) throws InvalidVolumeException {
      // 必须至少设置存储卷名称或ID其中一项
      if (Strings.isNullOrEmpty(spec.getVolumeName())
          && spec.getVolumeId() == null) {
        throw new InvalidVolumeException("Invalid volume, both volume name"
            + " and ID are missing from the spec. Volume spec: "
            + spec.toString());
      }
      // 必须设置存储卷容量信息
      if (spec.getVolumeCapabilityRange() == null) {
        throw new InvalidVolumeException("Invalid volume, volume capability"
            + " is missing. Volume spec: " + spec.toString());
      }
      // 必须设置CSI驱动名称
      if (Strings.isNullOrEmpty(spec.getDriverName())) {
        throw new InvalidVolumeException("Invalid volume, the csi-driver name"
            + " is missing. Volume spec: " + spec.toString());
      }
      // 必须设置挂载点
      if (Strings.isNullOrEmpty(spec.getMountPoint())) {
        throw new InvalidVolumeException("Invalid volume, the mount point"
            + " is missing. Volume spec: " + spec.toString());
      }
    }
  }
}