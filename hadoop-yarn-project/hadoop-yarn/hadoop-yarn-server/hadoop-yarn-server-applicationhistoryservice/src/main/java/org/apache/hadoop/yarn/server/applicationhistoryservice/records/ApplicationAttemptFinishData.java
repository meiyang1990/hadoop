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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.util.Records;

/**
 * 应用尝试完成数据，保存RM应用尝试结束时确定的、需要持久化存储的所有字段，用于应用历史服务存储
 */
@Public
@Unstable
public abstract class ApplicationAttemptFinishData {

  /**
   * 创建新的应用尝试完成数据实例
   * @param appAttemptId 应用尝试ID
   * @param diagnosticsInfo 诊断信息
   * @param trackingURL 追踪URL
   * @param finalApplicationStatus 应用最终状态
   * @param yarnApplicationAttemptState YARN应用尝试状态
   * @return 初始化完成的应用尝试完成数据实例
   */
  @Public
  @Unstable
  public static ApplicationAttemptFinishData newInstance(
      ApplicationAttemptId appAttemptId, String diagnosticsInfo,
      String trackingURL, FinalApplicationStatus finalApplicationStatus,
      YarnApplicationAttemptState yarnApplicationAttemptState) {
    ApplicationAttemptFinishData appAttemptFD =
        Records.newRecord(ApplicationAttemptFinishData.class);
    appAttemptFD.setApplicationAttemptId(appAttemptId);
    appAttemptFD.setDiagnosticsInfo(diagnosticsInfo);
    appAttemptFD.setTrackingURL(trackingURL);
    appAttemptFD.setFinalApplicationStatus(finalApplicationStatus);
    appAttemptFD.setYarnApplicationAttemptState(yarnApplicationAttemptState);
    return appAttemptFD;
  }

  @Public
  @Unstable
  public abstract ApplicationAttemptId getApplicationAttemptId();

  @Public
  @Unstable
  public abstract void setApplicationAttemptId(
      ApplicationAttemptId applicationAttemptId);

  @Public
  @Unstable
  public abstract String getTrackingURL();

  @Public
  @Unstable
  public abstract void setTrackingURL(String trackingURL);

  @Public
  @Unstable
  public abstract String getDiagnosticsInfo();

  @Public
  @Unstable
  public abstract void setDiagnosticsInfo(String diagnosticsInfo);

  @Public
  @Unstable
  public abstract FinalApplicationStatus getFinalApplicationStatus();

  @Public
  @Unstable
  public abstract void setFinalApplicationStatus(
      FinalApplicationStatus finalApplicationStatus);

  @Public
  @Unstable
  public abstract YarnApplicationAttemptState getYarnApplicationAttemptState();

  @Public
  @Unstable
  public abstract void setYarnApplicationAttemptState(
      YarnApplicationAttemptState yarnApplicationAttemptState);

}