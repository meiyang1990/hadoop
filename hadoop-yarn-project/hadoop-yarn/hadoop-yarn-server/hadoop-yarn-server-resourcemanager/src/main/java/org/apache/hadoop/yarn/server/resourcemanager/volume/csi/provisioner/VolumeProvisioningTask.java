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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner;

import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.ControllerPublishVolumeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.ValidateVolumeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * YARN CSI存储卷置备任务，封装存储系统预配卷所需的完整逻辑。
 * 这是通用实现，若特定存储系统的置备行为与默认实现不一致可继承重写。
 * 属于YARN ResourceManager侧CSI卷管理模块，负责批量完成存储卷的验证和发布流程。
 */
public class VolumeProvisioningTask implements VolumeProvisioner {

  private static final Logger LOG =  LoggerFactory
      .getLogger(VolumeProvisioningTask.class);

  // 需要本次置备的存储卷列表
  private List<Volume> volumes;

  /**
   * 构造存储卷置备任务，传入待处理的存储卷列表。
   * @param volumes 待置备的存储卷集合
   */
  public VolumeProvisioningTask(List<Volume> volumes) {
    this.volumes = volumes;
  }

  /**
   * 获取本次任务待处理的存储卷列表。
   * @return 待处理存储卷列表
   */
  public List<Volume> getVolumes() {
    return this.volumes;
  }

  @Override
  public VolumeProvisioningResults call() throws Exception {
    // 初始化置备结果收集容器
    VolumeProvisioningResults vpr = new VolumeProvisioningResults();

    // 遍历所有卷，依次完成状态流转
    for (Volume vs : volumes) {
      LOG.info("Provisioning volume : {}", vs.getVolumeId().toString());
      // 触发存储卷验证流程
      vs.handle(new ValidateVolumeEvent(vs));
      // 触发控制器发布存储卷流程
      vs.handle(new ControllerPublishVolumeEvent(vs));
    }

    // 收集所有卷的最终状态，生成置备结果
    volumes.stream().forEach(v ->
        vpr.addResult(v.getVolumeId(), v.getVolumeState()));

    return vpr;
  }
}