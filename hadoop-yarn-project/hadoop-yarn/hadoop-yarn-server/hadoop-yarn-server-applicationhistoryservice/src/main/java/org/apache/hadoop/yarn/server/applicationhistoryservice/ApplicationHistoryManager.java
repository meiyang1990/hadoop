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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 应用历史管理器接口，定义了查询应用、应用尝试和容器历史数据的标准 API。
 * 
 * 该接口提供了获取已完成或正在运行的应用历史信息的能力，
 * 包括应用报告、尝试列表、容器信息等。
 */
@Private
@Unstable
public interface ApplicationHistoryManager {
  /**
   * 获取指定应用的报告信息。
   * 
   * @param appId 应用 ID
   * @return 应用的报告信息
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  ApplicationReport getApplication(ApplicationId appId) throws YarnException,
      IOException;

  /**
   * 获取指定启动时间范围内的应用列表。
   * 
   * @param appsNum 返回的应用数量上限
   * @param appStartedTimeBegin 启动时间范围起点
   * @param appStartedTimeEnd 启动时间范围终点
   * @return 应用 ID 到应用报告的映射
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  Map<ApplicationId, ApplicationReport> getApplications(long appsNum,
      long appStartedTimeBegin, long appStartedTimeEnd) throws YarnException,
      IOException;

  /**
   * 获取指定应用的所有尝试列表。
   * 一个应用可能包含多次尝试，本方法返回该应用的所有尝试报告。
   * 
   * @param appId 应用 ID
   * @return 应用尝试 ID 到尝试报告的映射
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  Map<ApplicationAttemptId, ApplicationAttemptReport> getApplicationAttempts(
      ApplicationId appId) throws YarnException, IOException;

  /**
   * 获取指定应用尝试的报告信息。
   * 
   * @param appAttemptId 应用尝试 ID
   * @return 应用尝试报告
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException;

  /**
   * 获取指定容器的报告信息。
   * 
   * @param containerId 容器 ID
   * @return 容器报告
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  ContainerReport getContainer(ContainerId containerId) throws YarnException,
      IOException;

  /**
   * 获取指定应用尝试的 ApplicationMaster 容器报告。
   * 
   * @param appAttemptId 应用尝试 ID
   * @return AM 容器报告
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  ContainerReport getAMContainer(ApplicationAttemptId appAttemptId)
      throws YarnException, IOException;

  /**
   * 获取指定应用尝试的所有容器报告。
   * 
   * @param appAttemptId 应用尝试 ID
   * @return 容器 ID 到容器报告的映射
   * @throws YarnException
   * @throws IOException
   */
  @Public
  @Unstable
  Map<ContainerId, ContainerReport> getContainers(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException;

}
