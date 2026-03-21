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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;

/**
 * 封装YARN资源调度器尝试节点分配后返回的响应结果，用于约束调度流程中表示分配请求的处理结果
 */
public class SchedulingResponse {

  // 标识本次分配请求是否处理成功
  private final boolean isSuccess;
  // 对应请求所属应用ID
  private final ApplicationId applicationId;
  // 原始调度请求对象
  private final SchedulingRequest schedulingRequest;

  /**
   * 构造一个调度响应对象
   * @param isSuccess 调度器是否接受了该分配请求
   * @param applicationId 对应应用ID
   * @param schedulingRequest 原始调度请求
   */
  public SchedulingResponse(boolean isSuccess, ApplicationId applicationId,
      SchedulingRequest schedulingRequest) {
    this.isSuccess = isSuccess;
    this.applicationId = applicationId;
    this.schedulingRequest = schedulingRequest;
  }

  /**
   * 获取分配请求处理结果，true表示调度器成功接受并提交了该请求
   * @return 分配请求是否成功
   */
  public boolean isSuccess() {
    return this.isSuccess;
  }

  /**
   * 获取对应应用ID
   * @return 应用ID
   */
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  /**
   * 获取原始调度请求
   * @return 调度请求对象
   */
  public SchedulingRequest getSchedulingRequest() {
    return this.schedulingRequest;
  }

}