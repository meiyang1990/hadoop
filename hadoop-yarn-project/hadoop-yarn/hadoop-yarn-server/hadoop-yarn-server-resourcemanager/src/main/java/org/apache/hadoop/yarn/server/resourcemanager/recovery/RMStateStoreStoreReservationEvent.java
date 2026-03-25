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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;

/**
 * YARN ResourceManager 状态恢复: 持久化预留系统状态的事件
 * 用于将预留分配信息写入 RM 状态存储，支持 RM 故障恢复后恢复预留系统状态
 */
public class RMStateStoreStoreReservationEvent extends RMStateStoreEvent {

  // 预留分配状态Protobuf对象
  private ReservationAllocationStateProto reservationAllocation;
  // 预留所属资源计划名称
  private String planName;
  // 预留ID标识
  private String reservationIdName;

  /**
   * 构造函数，仅指定事件类型
   * @param type 状态存储事件类型
   */
  public RMStateStoreStoreReservationEvent(RMStateStoreEventType type) {
    super(type);
  }

  /**
   * 完整构造函数，包含预留所有信息
   * @param reservationAllocationState 预留分配状态对象
   * @param type 状态存储事件类型
   * @param planName 资源计划名称
   * @param reservationIdName 预留ID
   */
  public RMStateStoreStoreReservationEvent(
      ReservationAllocationStateProto reservationAllocationState,
      RMStateStoreEventType type, String planName, String reservationIdName) {
    this(type);
    this.reservationAllocation = reservationAllocationState;
    this.planName = planName;
    this.reservationIdName = reservationIdName;
  }

  /**
   * 获取预留分配状态对象
   * @return 预留分配状态Protobuf对象
   */
  public ReservationAllocationStateProto getReservationAllocation() {
    return reservationAllocation;
  }

  /**
   * 获取资源计划名称
   * @return 资源计划名称
   */
  public String getPlanName() {
    return planName;
  }

  /**
   * 获取预留ID
   * @return 预留ID标识
   */
  public String getReservationIdName() {
    return reservationIdName;
  }
}