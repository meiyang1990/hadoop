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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.processor;

import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningResults;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningTask;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;
import org.apache.hadoop.yarn.server.volume.csi.exception.VolumeException;
import org.apache.hadoop.yarn.server.volume.csi.exception.VolumeProvisioningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 处理ApplicationMaster CSI卷资源请求的AMS处理器，负责从 allocate 请求中提取卷请求并触发卷预配流程
 *
 */
public class VolumeAMSProcessor implements ApplicationMasterServiceProcessor {

  private static final Logger LOG =  LoggerFactory
      .getLogger(VolumeAMSProcessor.class);

  // 责任链中的下一个处理器
  private ApplicationMasterServiceProcessor nextAMSProcessor;
  // CSI卷管理器实例
  private VolumeManager volumeManager;

  @Override
  public void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor) {
    LOG.info("Initializing CSI volume processor");
    this.nextAMSProcessor = nextProcessor;
    this.volumeManager = ((RMContext) amsContext).getVolumeManager();
  }

  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException {
    this.nextAMSProcessor.registerApplicationMaster(applicationAttemptId,
        request, response);
  }

  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    // 从allocate请求中聚合所有待预配的卷
    List<Volume> volumes = aggregateVolumesFrom(request);
    // 如果存在需要预配的卷
    if (volumes != null && volumes.size() > 0) {
      // 提交卷预配任务到卷管理器调度执行
      ScheduledFuture<VolumeProvisioningResults> result =
          this.volumeManager.schedule(new VolumeProvisioningTask(volumes), 0);
      try {
        // 等待预配结果，设置3秒超时
        VolumeProvisioningResults volumeResult =
            result.get(3, TimeUnit.SECONDS);
        // 预配失败抛出异常
        if (!volumeResult.isSuccess()) {
          throw new VolumeProvisioningException("Volume provisioning failed,"
              + " result details: " + volumeResult.getBriefMessage());
        }
      } catch (TimeoutException | InterruptedException | ExecutionException e) {
        LOG.warn("Volume provisioning task failed", e);
        throw new VolumeException("Volume provisioning task failed", e);
      }
    }

    // 传递请求给责任链下一个处理器
    this.nextAMSProcessor.allocate(appAttemptId, request, response);
  }

  // Currently only scheduling request is supported.
  /**
   * 从Allocate请求的调度请求中提取并聚合所有卷元数据，生成待处理Volume列表
   */
  private List<Volume> aggregateVolumesFrom(AllocateRequest request)
      throws VolumeException {
    List<Volume> volumeList = new ArrayList<>();
    List<SchedulingRequest> requests = request.getSchedulingRequests();
    if (requests != null) {
      // 遍历所有调度请求
      for (SchedulingRequest req : requests) {
        // 获取请求资源
        Resource totalResource = req.getResourceSizing().getResources();
        // 获取所有资源信息
        List<ResourceInformation> resourceList =
            totalResource.getAllResourcesListCopy();
        // 遍历每个资源信息提取卷元数据
        for (ResourceInformation resourceInformation : resourceList) {
          List<VolumeMetaData> volumes =
              VolumeMetaData.fromResource(resourceInformation);
          // 处理每个卷元数据
          for (VolumeMetaData vs : volumes) {
            // 容量未指定，跳过该卷
            if (vs.getVolumeCapabilityRange().getMinCapacity() <= 0) {
              // capacity not specified, ignore
              continue;
            } else if (vs.isProvisionedVolume()) {
              // 已预配卷，验证后添加到处理列表
              volumeList.add(checkAndGetVolume(vs));
            } else {
              // 当前不支持动态创建卷，抛出异常
              throw new InvalidVolumeException("Only pre-provisioned volume"
                  + " is supported now, volumeID must exist.");
            }
          }
        }
      }
    }
    return volumeList;
  }

  /**
   * If given volume ID already exists in the volume manager,
   * it returns the existing volume. Otherwise, it creates a new
   * volume and add that to volume manager.
   * @param metaData 卷元数据
   * @return 验证后的Volume实例
   * @throws InvalidVolumeException 当找不到对应CSI驱动适配器时抛出
   */
  private Volume checkAndGetVolume(VolumeMetaData metaData)
      throws InvalidVolumeException {
    Volume toAdd = new VolumeImpl(metaData);
    // 根据驱动名称获取对应CSI适配器
    CsiAdaptorProtocol adaptor = volumeManager
        .getAdaptorByDriverName(metaData.getDriverName());
    if (adaptor == null) {
      throw new InvalidVolumeException("It seems for the driver name"
          + " specified in the volume " + metaData.getDriverName()
          + " ,there is no matched driver-adaptor can be found. "
          + "Is the driver probably registered? Please check if"
          + " adaptors service addresses defined in "
          + YarnConfiguration.NM_CSI_ADAPTOR_ADDRESSES
          + " are correct and services are started.");
    }
    // 设置适配器客户端
    toAdd.setClient(adaptor);
    // 添加到卷管理器，已存在则返回已有实例
    return this.volumeManager.addOrGetVolume(toAdd);
  }

  @Override
  public void finishApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {
    this.nextAMSProcessor.finishApplicationMaster(applicationAttemptId,
        request, response);
  }
}