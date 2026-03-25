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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;

/**
 * CSI存储卷事件基类，用于触发存储卷状态转换，是所有CSI卷事件的父类。
 */
public class VolumeEvent extends AbstractEvent<VolumeEventType> {

  // 关联的CSI存储卷对象
  private Volume volume;

  /**
   * 构造CSI存储卷事件。
   * @param volume 关联的存储卷对象
   * @param volumeEventType 存储卷事件类型
   */
  public VolumeEvent(Volume volume, VolumeEventType volumeEventType) {
    super(volumeEventType, System.currentTimeMillis());
    this.volume = volume;
  }

  /**
   * 获取当前事件关联的存储卷对象。
   * @return 关联的存储卷对象
   */
  public Volume getVolume() {
    return this.volume;
  }

  /**
   * 获取当前事件关联存储卷的唯一ID。
   * @return 存储卷唯一ID
   */
  public VolumeId getVolumeId() {
    return this.volume.getVolumeId();
  }
}