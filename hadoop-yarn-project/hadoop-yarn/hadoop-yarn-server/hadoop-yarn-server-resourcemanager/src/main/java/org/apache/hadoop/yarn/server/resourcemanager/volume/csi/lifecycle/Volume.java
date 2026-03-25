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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.VolumeEvent;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;

/**
 * YARN RM侧存储卷抽象接口，维护存储卷生命周期状态，根据CSI规范处理状态流转
 */
@Private
@Unstable
public interface Volume extends EventHandler<VolumeEvent> {

  /**
   * 获取存储卷当前生命周期状态
   * @return 存储卷当前状态
   */
  VolumeState getVolumeState();

  /**
   * 获取存储卷唯一标识ID
   * @return 存储卷ID
   */
  VolumeId getVolumeId();

  /**
   * 获取存储卷元数据信息
   * @return 存储卷元数据
   */
  VolumeMetaData getVolumeMeta();

  /**
   * 获取CSI适配器客户端，用于和CSI适配器通信
   * @return CSI适配器协议客户端
   */
  CsiAdaptorProtocol getClient();

  /**
   * 设置CSI适配器客户端
   * @param client CSI适配器协议客户端
   */
  void setClient(CsiAdaptorProtocol client);
}