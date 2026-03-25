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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.util.Records;

/**
 * 日志聚合报告，用于记录应用在单个NodeManager上的日志聚合状态信息。
 * <p>
 * 包含以下核心信息：
 * <ul>
 *   <li>应用ID {@link ApplicationId}</li>
 *   <li>日志聚合状态 {@link LogAggregationStatus}</li>
 *   <li>诊断信息，用于记录聚合过程中的异常或提示</li>
 * </ul>
 *
 */
@Public
@Unstable
public abstract class LogAggregationReport {

  /**
   * 创建一个新的日志聚合报告实例。
   * @param appId 应用ID
   * @param status 日志聚合状态
   * @param diagnosticMessage 诊断信息
   * @return 日志聚合报告实例
   */
  @Public
  @Unstable
  public static LogAggregationReport newInstance(ApplicationId appId,
      LogAggregationStatus status, String diagnosticMessage) {
    LogAggregationReport report = Records.newRecord(LogAggregationReport.class);
    report.setApplicationId(appId);
    report.setLogAggregationStatus(status);
    report.setDiagnosticMessage(diagnosticMessage);
    return report;
  }

  /**
   * 获取应用ID。
   * @return 应用ID
   */
  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  @Public
  @Unstable
  public abstract void setApplicationId(ApplicationId appId);

  /**
   * 获取日志聚合状态。
   * @return 日志聚合状态
   */
  @Public
  @Unstable
  public abstract LogAggregationStatus getLogAggregationStatus();

  @Public
  @Unstable
  public abstract void setLogAggregationStatus(
      LogAggregationStatus logAggregationStatus);

  /**
   * 获取日志聚合的诊断信息。
   * @return 日志聚合诊断信息
   */
  @Public
  @Unstable
  public abstract String getDiagnosticMessage();

  @Public
  @Unstable
  public abstract void setDiagnosticMessage(String diagnosticMessage);
}