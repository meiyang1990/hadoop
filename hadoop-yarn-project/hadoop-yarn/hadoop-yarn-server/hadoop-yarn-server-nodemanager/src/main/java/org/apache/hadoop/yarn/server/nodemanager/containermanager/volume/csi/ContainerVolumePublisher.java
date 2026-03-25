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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.volume.csi;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.OCIContainerRuntime;
import org.apache.hadoop.yarn.server.volume.csi.CsiConstants;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 在NodeManager节点上完成CSI卷的发布和回收清理，为容器挂载存储卷做准备。
 * 负责调用CSI适配器完成节点层面的卷挂载操作，并生成容器内的挂载绑定关系。
 */
public class ContainerVolumePublisher {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerVolumePublisher.class);

  private final Container container;
  private final String localMountRoot;
  private final OCIContainerRuntime runtime;

  /**
   * 构造容器卷发布器，绑定目标容器、本地挂载根目录和OCI容器运行时。
   * @param container 目标容器
   * @param localMountRoot 本地挂载根目录
   * @param runtime OCI容器运行时
   */
  public ContainerVolumePublisher(Container container, String localMountRoot,
      OCIContainerRuntime runtime) {
    LOG.info("Initiate container volume publisher, containerID={},"
            + " volume local mount rootDir={}",
        container.getContainerId().toString(), localMountRoot);
    this.container = container;
    this.localMountRoot = localMountRoot;
    this.runtime = runtime;
  }

  /**
   * 发布容器所需的所有CSI卷到当前NodeManager节点。
   * 第一阶段（控制器创建卷）已在RM侧完成，本方法仅完成第二阶段节点层面挂载。
   * 遍历所有容器请求的CSI卷，逐个完成节点挂载，生成本地路径到容器内路径的绑定映射。
   *
   * @return 挂载绑定映射，key为节点本地挂载路径，value为容器内目标挂载路径
   * @throws YarnException YARN服务异常
   * @throws IOException IO操作异常
   */
  public Map<String, String> publishVolumes() throws YarnException,
      IOException {
    LOG.info("publishing volumes");
    Map<String, String> volumeMounts = new HashMap<>();
    List<VolumeMetaData> volumes = getVolumes();
    LOG.info("Found {} volumes to be published on this node", volumes.size());
    // 遍历所有需要挂载的卷，逐个发布
    for (VolumeMetaData volume : volumes) {
      Map<String, String> bindings = publishVolume(volume);
      if (bindings != null && !bindings.isEmpty()) {
        volumeMounts.putAll(bindings);
      }
    }
    return volumeMounts;
  }

  /**
   * 卸载回收容器使用的所有CSI卷，容器退出后清理挂载。
   * @throws YarnException YARN服务异常
   * @throws IOException IO操作异常
   */
  public void unpublishVolumes() throws YarnException, IOException {
    LOG.info("Un-publishing Volumes");
    List<VolumeMetaData> volumes = getVolumes();
    LOG.info("Volumes to un-publish {}", volumes.size());
    // 遍历所有需要卸载的卷，逐个卸载
    for (VolumeMetaData volume : volumes) {
      this.unpublishVolume(volume);
    }
  }

  /**
   * 生成卷在节点本地的挂载路径。
   * @param containerWorkDir 容器工作目录
   * @param volumeId 卷ID
   * @return 本地挂载目录文件对象
   */
  private File getLocalVolumeMountPath(
      String containerWorkDir, String volumeId) {
    return new File(containerWorkDir, volumeId + "_mount");
  }

  /**
   * 生成卷在节点本地的临时 staging 路径。
   * @param containerWorkDir 容器工作目录
   * @param volumeId 卷ID
   * @return 本地staging目录文件对象
   */
  private File getLocalVolumeStagingPath(
      String containerWorkDir, String volumeId) {
    return new File(containerWorkDir, volumeId + "_staging");
  }

  /**
   * 从容器资源信息中提取所有需要挂载的CSI卷元数据。
   * @return CSI卷元数据列表
   * @throws InvalidVolumeException 卷信息无效异常
   */
  private List<VolumeMetaData> getVolumes() throws InvalidVolumeException {
    List<VolumeMetaData> volumes = new ArrayList<>();
    Resource containerResource = container.getResource();
    // 遍历容器所有资源信息，筛选出标记为CSI卷的资源
    if (containerResource != null) {
      for (ResourceInformation resourceInformation :
          containerResource.getAllResourcesListCopy()) {
        if (resourceInformation.getTags()
            .contains(CsiConstants.CSI_VOLUME_RESOURCE_TAG)) {
          volumes.addAll(VolumeMetaData.fromResource(resourceInformation));
        }
      }
    }
    if (volumes.size() > 0) {
      LOG.info("Total number of volumes require provisioning is {}",
          volumes.size());
    }
    return volumes;
  }

  /**
   * 发布单个CSI卷到当前节点，完成节点层面挂载。
   * @param volume 卷元数据
   * @return 挂载绑定映射，key为节点本地挂载路径，value为容器内目标挂载路径
   * @throws IOException IO操作异常
   * @throws YarnException YARN服务异常
   */
  private Map<String, String> publishVolume(VolumeMetaData volume)
      throws IOException, YarnException {
    Map<String, String> bindVolumes = new HashMap<>();
    // 生成本地挂载路径和临时staging路径
    File localMount = getLocalVolumeMountPath(
        localMountRoot, volume.getVolumeId().toString());
    File localStaging = getLocalVolumeStagingPath(
        localMountRoot, volume.getVolumeId().toString());
    LOG.info("Volume {}, local mount path: {}, local staging path {}",
        volume.getVolumeId().toString(), localMount, localStaging);

    // 构造节点发布卷请求，指定单节点可写、文件系统类型的卷能力
    NodePublishVolumeRequest publishRequest = NodePublishVolumeRequest
        .newInstance(volume.getVolumeId().getId(), // 卷ID
            false, // 只读标志，默认非只读
            localMount.getAbsolutePath(), // 节点侧目标挂载路径
            localStaging.getAbsolutePath(), // 临时staging路径
            new ValidateVolumeCapabilitiesRequest.VolumeCapability(
                ValidateVolumeCapabilitiesRequest
                    .AccessMode.SINGLE_NODE_WRITER,
                ValidateVolumeCapabilitiesRequest.VolumeType.FILE_SYSTEM,
                ImmutableList.of()), // 卷能力描述
            ImmutableMap.of(), // 发布上下文参数
            ImmutableMap.of());  // 密钥信息

    // 检查对应驱动的CSI适配器客户端是否存在
    if (runtime.getCsiClients().get(volume.getDriverName()) == null) {
      throw new YarnException("No csi-adaptor is found that can talk"
          + " to csi-driver " + volume.getDriverName());
    }

    // 调用CSI适配器完成节点层面的卷发布
    LOG.info("Publish volume on NM, request {}",
        publishRequest.toString());
    runtime.getCsiClients().get(volume.getDriverName())
        .nodePublishVolume(publishRequest);
    // 挂载成功后，添加绑定关系，供容器启动时挂载
    String containerMountPath = volume.getMountPoint();
    bindVolumes.put(localMount.getAbsolutePath(), containerMountPath);
    return bindVolumes;
  }

  /**
   * 卸载单个CSI卷，清理节点上的挂载。
   * @param volume 卷元数据
   * @throws YarnException YARN服务异常
   * @throws IOException IO操作异常
   */
  private void unpublishVolume(VolumeMetaData volume)
      throws YarnException, IOException {
    // 获取对应驱动的CSI适配器客户端
    CsiAdaptorProtocol csiClient =
        runtime.getCsiClients().get(volume.getDriverName());
    if (csiClient == null) {
      throw new YarnException(
          "No csi-adaptor is found that can talk"
              + " to csi-driver " + volume.getDriverName());
    }

    // 获取节点本地挂载路径
    File localMount = getLocalVolumeMountPath(container.getCsiVolumesRootDir(),
        volume.getVolumeId().toString());
    // 如果挂载路径已不存在，跳过清理
    if (!localMount.exists()) {
      LOG.info("Local mount {} no longer exist, skipping cleaning"
          + " up the volume", localMount.getAbsolutePath());
      return;
    }
    // 构造节点卸载卷请求
    NodeUnpublishVolumeRequest unpublishRequest =
        NodeUnpublishVolumeRequest.newInstance(
            volume.getVolumeId().getId(), // 卷ID
            localMount.getAbsolutePath());  // 目标挂载路径

    // 调用CSI适配器完成节点层面的卷卸载
    LOG.info("Un-publish volume {}, request {}",
        volume.getVolumeId().toString(), unpublishRequest.toString());
    csiClient.nodeUnpublishVolume(unpublishRequest);
  }
}