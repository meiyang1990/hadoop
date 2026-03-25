// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api;

import org.apache.hadoop.yarn.api.records.SchedulingRequest;

/**
 * YARN 资源调度约束 API 模块：承载带有放置尝试次数的调度请求
 * 该类用于在调度位置约束计算过程中，包装原始调度请求和当前尝试次数信息
 */
public class SchedulingRequestWithPlacementAttempt {

  private final int placementAttempt;
  private final SchedulingRequest schedulingRequest;

  /**
   * 构造携带放置尝试次数的调度请求包装对象
   * @param placementAttempt 当前放置尝试次数
   * @param schedulingRequest 原始调度请求对象
   */
  public SchedulingRequestWithPlacementAttempt(int placementAttempt,
      SchedulingRequest schedulingRequest) {
    this.placementAttempt = placementAttempt;
    this.schedulingRequest = schedulingRequest;
  }

  /**
   * 获取当前放置尝试次数
   * @return 放置尝试次数
   */
  public int getPlacementAttempt() {
    return placementAttempt;
  }

  /**
   * 获取原始调度请求对象
   * @return 原始调度请求
   */
  public SchedulingRequest getSchedulingRequest() {
    return schedulingRequest;
  }

  @Override
  public String toString() {
    return "SchedulingRequestWithPlacementAttempt{" +
        "placementAttempt=" + placementAttempt +
        ", schedulingRequest=" + schedulingRequest +
        '}';
  }
}