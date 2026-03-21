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
 * CSI存储卷验证事件，用于触发与CSI驱动交互验证存储卷能力。
 * 属于YARN CSI存储卷生命周期管理的事件体系，用于请求验证存储卷是否满足应用需求。
 */
public class ValidateVolumeEvent extends VolumeEvent {

  /**
   * 构造存储卷验证事件。
   * @param volume 待验证的CSI存储卷对象
   */
  public ValidateVolumeEvent(Volume volume) {
    super(volume, VolumeEventType.VALIDATE_VOLUME_EVENT);
  }
}