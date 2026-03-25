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

/**
 * YARN CSI卷生命周期状态枚举
 * 卷状态定义遵循CSI规范，描述了卷从创建到可用整个生命周期的不同阶段
 */
public enum VolumeState {
  /** 初始状态，卷刚被创建 */
  NEW,
  /** 卷容量已验证通过 */
  VALIDATED,
  /** 卷已由CSI控制器创建完成 */
  CREATED,
  /** 卷已发布到节点，节点就绪可使用 */
  NODE_READY,
  /** 卷不可用 */
  UNAVAILABLE
}