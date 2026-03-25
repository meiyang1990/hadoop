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
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * YARN CSI存储卷状态管理器，维护集群中所有已管理存储卷的信息与状态。
 * 为CSI存储卷的生命周期管理提供全局状态存储能力。
 */
public class VolumeStates {

  // 存储卷ID到存储卷实例的映射表，存储所有已管理的存储卷
  private final Map<VolumeId, Volume> volumeStates;

  /**
   * 构造存储卷状态管理器，初始化并发存储容器。
   */
  public VolumeStates() {
    this.volumeStates = new ConcurrentHashMap<>();
  }

  /**
   * 根据存储卷ID获取对应的存储卷实例。
   * @param volumeId 存储卷唯一ID
   * @return 对应存储卷实例，不存在则返回null
   */
  public Volume getVolume(VolumeId volumeId) {
    return volumeStates.get(volumeId);
  }

  /**
   * 当存储卷不存在时添加新存储卷。若已存在同ID存储卷，返回已有存储卷。
   * 支持动态制备存储卷的场景：动态制备的存储卷在创建时可能还未生成实际ID，会先返回临时实例。
   * @param volume 待添加的存储卷实例
   * @return 若添加成功返回null；若已存在同ID存储卷返回已有实例；无ID时返回原传入实例
   */
  public Volume addVolumeIfAbsent(Volume volume) {
    if (volume.getVolumeId() != null) {
      return volumeStates.putIfAbsent(volume.getVolumeId(), volume);
    } else {
      // 动态制备的存储卷，创建时可能还未分配实际Volume ID
      // 后续生成正式ID后会更新替换，此处先返回原实例
      return volume;
    }
  }
}