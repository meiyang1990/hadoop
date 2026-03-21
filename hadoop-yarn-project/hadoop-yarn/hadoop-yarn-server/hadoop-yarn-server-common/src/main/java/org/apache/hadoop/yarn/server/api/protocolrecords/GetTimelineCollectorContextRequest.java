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
package org.apache.hadoop.yarn.server.api.protocolrecords;


import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * 获取时间线收集器上下文请求，用于从YARN服务获取指定应用的时间线收集器配置上下文信息。
 */
public abstract class GetTimelineCollectorContextRequest {

  /**
   * 创建获取时间线收集器上下文请求实例。
   * @param appId 目标应用ID
   * @return 构建完成的请求对象
   */
  public static GetTimelineCollectorContextRequest newInstance(
      ApplicationId appId) {
    GetTimelineCollectorContextRequest request =
        Records.newRecord(GetTimelineCollectorContextRequest.class);
    request.setApplicationId(appId);
    return request;
  }

  /**
   * 获取请求目标应用ID。
   * @return 目标应用ID
   */
  public abstract ApplicationId getApplicationId();

  /**
   * 设置请求目标应用ID。
   * @param appId 目标应用ID
   */
  public abstract void setApplicationId(ApplicationId appId);
}