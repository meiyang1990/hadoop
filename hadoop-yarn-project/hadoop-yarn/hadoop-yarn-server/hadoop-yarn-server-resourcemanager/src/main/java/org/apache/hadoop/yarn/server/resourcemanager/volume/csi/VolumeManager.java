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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningResults;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningTask;

import java.util.concurrent.ScheduledFuture;

/**
 * YARN CSI存储卷管理器核心接口，负责管理集群中所有存储卷的全生命周期。
 * 存储卷会先通过CSI控制器插件完成准备操作，之后才能发布到NodeManager上供容器使用。
 */
@Private
@Unstable
public interface VolumeManager {

  /**
   * 获取所有已知存储卷及其当前状态信息。
   * @return 所有存储卷及其状态集合
   */
  VolumeStates getVolumeStates();

  /**
   * 将指定存储卷添加到管理中，并启动状态监控。如果已存在则返回已有实例。
   * @param volume 待管理的存储卷
   * @return 被管理器管理的存储卷实例（新增或已有）
   */
  Volume addOrGetVolume(Volume volume);

  /**
   * 异步调度执行存储卷制备任务，在后台线程中完成制备流程。
   * @param volumeProvisioningTask 封装了特定存储系统制备存储卷所需全部逻辑的任务
   * @param delaySecond 调度延迟秒数
   * @return 异步任务的Future对象，可用于获取执行结果
   */
  ScheduledFuture<VolumeProvisioningResults> schedule(
      VolumeProvisioningTask volumeProvisioningTask, int delaySecond);

  /**
   * 向管理器注册指定CSI驱动对应的适配器客户端。
   * @param driverName CSI驱动名称
   * @param client CSI适配器协议客户端实例
   */
  void registerCsiDriverAdaptor(String driverName, CsiAdaptorProtocol client);

  /**
   * 根据驱动名称从缓存中获取对应的CSI驱动适配器客户端。
   * @param driverName CSI驱动名称
   * @return 对应驱动的适配器客户端，如果未找到则返回null
   */
  CsiAdaptorProtocol getAdaptorByDriverName(String driverName);
}