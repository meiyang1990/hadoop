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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * YARN RM 禁用型应用位置约束处理处理器，拒绝所有包含位置约束的调度请求。
 * 当未开启位置约束处理功能时使用该实现，拦截并拒绝包含位置约束的请求。
 */
public class DisabledPlacementProcessor extends AbstractPlacementProcessor {
  private static final Logger LOG =
      LoggerFactory.getLogger(DisabledPlacementProcessor.class);

  /**
   * 注册ApplicationMaster，检查请求是否包含位置约束，有则拒绝，否则传递给下一级处理器。
   * @param applicationAttemptId 应用尝试ID
   * @param request 注册请求
   * @param response 注册响应
   * @throws IOException IO异常
   * @throws YarnException YARN异常，当请求包含位置约束时抛出
   */
  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException {
    // 检查请求是否携带非空位置约束
    if (request.getPlacementConstraints() != null && !request
        .getPlacementConstraints().isEmpty()) {
      String message = "Found non empty placement constraints map in "
          + "RegisterApplicationMasterRequest for application="
          + applicationAttemptId.toString() + ", but the configured "
          + YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " cannot handle placement constraints. Rejecting this "
          + "registerApplicationMaster operation";
      LOG.warn(message);
      throw new YarnException(message);
    }
    // 无位置约束，传递给下一级处理器继续处理
    nextAMSProcessor.registerApplicationMaster(applicationAttemptId, request,
        response);
  }

  /**
   * 处理资源分配请求，检查请求是否包含调度请求/位置约束，有则拒绝，否则传递给下一级处理器。
   * @param appAttemptId 应用尝试ID
   * @param request 分配请求
   * @param response 分配响应
   * @throws YarnException YARN异常，当请求包含调度请求/位置约束时抛出
   */
  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    // 检查请求是否携带非空调度请求（包含位置约束）
    if (request.getSchedulingRequests() != null && !request
        .getSchedulingRequests().isEmpty()) {
      String message = "Found non empty SchedulingRequest in "
          + "AllocateRequest for application="
          + appAttemptId.toString() + ", but the configured "
          + YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " cannot handle placement constraints. Rejecting this "
          + "allocate operation";
      LOG.warn(message);
      throw new YarnException(message);
    }
    // 无位置约束，传递给下一级处理器继续处理
    nextAMSProcessor.allocate(appAttemptId, request, response);
  }
}