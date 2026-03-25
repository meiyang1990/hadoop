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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN调度器放置约束处理链中的调度器原生放置处理器，
 * 检查调度器是否支持调度请求，若支持则将调度放置请求转发给调度器处理。
 * 仅当调度器本身支持SchedulingRequests时，才会转发请求。
 */
public class SchedulerPlacementProcessor extends AbstractPlacementProcessor {
  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulerPlacementProcessor.class);

  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    // 检查请求中是否包含非空的调度请求
    if (request.getSchedulingRequests() != null
        && !request.getSchedulingRequests().isEmpty()) {
      // 检查当前调度器是否开启了放置约束支持
      if (!scheduler.placementConstraintEnabled()) {
        // 构造错误信息，记录警告日志并抛出异常拒绝分配请求
        String message = "Found non empty SchedulingRequest of "
            + "AllocateRequest for application=" + appAttemptId.toString()
            + ", however the configured scheduler="
            + scheduler.getClass().getCanonicalName()
            + " cannot handle placement constraints, rejecting this "
            + "allocate operation";
        LOG.warn(message);
        throw new YarnException(message);
      }
    }
    // 校验通过，将请求转发给处理链下一个处理器继续处理
    nextAMSProcessor.allocate(appAttemptId, request, response);
  }
}