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

import java.util.List;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.util.Records;

/**
 * 上报新的应用时间线采集器信息的请求类
 * 用于向ResourceManager注册新增的应用采集器，让集群能够感知采集器地址和认证信息
 */
@Private
public abstract class ReportNewCollectorInfoRequest {

  /**
   * 创建上报新采集器信息请求实例
   * @param appCollectorsList 应用采集器信息列表
   * @return 新的请求对象
   */
  public static ReportNewCollectorInfoRequest newInstance(
      List<AppCollectorData> appCollectorsList) {
    ReportNewCollectorInfoRequest request =
        Records.newRecord(ReportNewCollectorInfoRequest.class);
    request.setAppCollectorsList(appCollectorsList);
    return request;
  }

  /**
   * 创建单应用采集器上报请求实例
   * @param id 应用ID
   * @param collectorAddr 采集器服务地址
   * @param token 采集器访问认证令牌
   * @return 新的请求对象
   */
  public static ReportNewCollectorInfoRequest newInstance(
      ApplicationId id, String collectorAddr, Token token) {
    ReportNewCollectorInfoRequest request =
        Records.newRecord(ReportNewCollectorInfoRequest.class);
    request.setAppCollectorsList(
        Arrays.asList(AppCollectorData.newInstance(id, collectorAddr, token)));
    return request;
  }

  /**
   * 获取需要上报的应用采集器信息列表
   * @return 应用采集器信息列表
   */
  public abstract List<AppCollectorData> getAppCollectorsList();

  /**
   * 设置需要上报的应用采集器信息列表
   * @param appCollectorsList 应用采集器信息列表
   */
  public abstract void setAppCollectorsList(
      List<AppCollectorData> appCollectorsList);

}