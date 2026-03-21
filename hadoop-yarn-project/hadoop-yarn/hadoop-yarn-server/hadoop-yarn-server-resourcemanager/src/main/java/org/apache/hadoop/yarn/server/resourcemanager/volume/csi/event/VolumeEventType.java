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

/**
 * YARN CSI卷管理事件类型枚举，定义了存储卷生命周期中各类事件的类型。
 * 用于RM中CSI卷管理模块的事件驱动流程，标识不同的卷操作请求。
 */
public enum VolumeEventType {
  /** 卷合法性校验事件 */
  VALIDATE_VOLUME_EVENT,
  /** 创建存储卷事件 */
  CREATE_VOLUME_EVENT,
  /** 控制器端发布存储卷事件（将卷挂载到目标节点） */
  CONTROLLER_PUBLISH_VOLUME_EVENT,
  /** 控制器端取消发布存储卷事件（从目标节点卸载卷） */
  CONTROLLER_UNPUBLISH_VOLUME_EVENT,
  /** 删除存储卷事件 */
  DELETE_VOLUME
}