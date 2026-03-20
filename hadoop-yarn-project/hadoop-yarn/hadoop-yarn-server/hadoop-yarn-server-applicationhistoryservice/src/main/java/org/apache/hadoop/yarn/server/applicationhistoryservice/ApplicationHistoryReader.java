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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;

/**
 * 应用历史数据读取器接口，定义了读取应用历史数据的标准 API。
 * 
 * 该接口提供了从存储后端读取应用、应用尝试和容器历史数据的能力。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface ApplicationHistoryReader {

  /**
   * 获取指定应用的历史数据。
   * 
   * @param appId 应用 ID
   * @return 应用历史数据
   * @throws IOException
   */
  ApplicationHistoryData getApplication(ApplicationId appId) throws IOException;

  /**
   * 获取所有应用的历史数据。
   * 
   * @return 应用 ID 到应用历史数据的映射
   * @throws IOException
   */
  Map<ApplicationId, ApplicationHistoryData> getAllApplications()
      throws IOException;

  /**
   * 获取指定应用的所有尝试历史数据。
   * 一个应用可能包含多次尝试。
   * 
   * @param appId 应用 ID
   * @return 应用尝试 ID 到尝试历史数据的映射
   * @throws IOException
   */
  Map<ApplicationAttemptId, ApplicationAttemptHistoryData>
      getApplicationAttempts(ApplicationId appId) throws IOException;

  /**
   * 获取指定应用尝试的历史数据。
   * 
   * @param appAttemptId 应用尝试 ID
   * @return 应用尝试历史数据
   * @throws IOException
   */
  ApplicationAttemptHistoryData getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws IOException;

  /**
   * 获取指定容器的历史数据。
   * 
   * @param containerId 容器 ID
   * @return 容器历史数据
   * @throws IOException
   */
  ContainerHistoryData getContainer(ContainerId containerId) throws IOException;

  /**
   * 获取指定应用尝试的 AM 容器历史数据。
   * 
   * @param appAttemptId 应用尝试 ID
   * @return AM 容器历史数据
   * @throws IOException
   */
  ContainerHistoryData getAMContainer(ApplicationAttemptId appAttemptId)
      throws IOException;

  /**
   * 获取指定应用尝试的所有容器历史数据。
   * 
   * @param appAttemptId 应用尝试 ID
   * @return 容器 ID 到容器历史数据的映射
   * @throws IOException
   */
  Map<ContainerId, ContainerHistoryData> getContainers(
      ApplicationAttemptId appAttemptId) throws IOException;
}
