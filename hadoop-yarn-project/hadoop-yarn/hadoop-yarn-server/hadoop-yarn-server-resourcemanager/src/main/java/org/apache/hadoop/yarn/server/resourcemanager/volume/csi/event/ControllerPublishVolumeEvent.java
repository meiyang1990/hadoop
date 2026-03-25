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

import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;

/**
 * CSI卷控制器发布事件，触发CSI控制器执行卷发布操作，将卷准备到指定节点。
 * 属于YARN CSI存储卷生命周期管理中的核心事件。
 */
public class ControllerPublishVolumeEvent extends VolumeEvent {

  /**
   * 构造控制器发布卷事件，绑定目标卷对象。
   * @param volume 待发布的CSI卷对象
   */
  public ControllerPublishVolumeEvent(Volume volume) {
    super(volume, VolumeEventType.CONTROLLER_PUBLISH_VOLUME_EVENT);
  }
}