// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * RMApp 应用使用的日志聚合状态管理逻辑，负责跟踪整个应用在各NodeManager上的日志聚合进度与状态。
 *
 */
public class RMAppLogAggregation {
  private final boolean logAggregationEnabled;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private long logAggregationStartTime = 0;
  private final long logAggregationStatusTimeout;
  // 存储每个NodeManager对应的日志聚合报告
  private final Map<NodeId, LogAggregationReport> logAggregationStatus =
      new ConcurrentHashMap<>();
  private volatile LogAggregationStatus logAggregationStatusForAppReport;
  private int logAggregationSucceed = 0;
  private int logAggregationFailed = 0;
  // 存储每个NodeManager的日志聚合诊断信息
  private Map<NodeId, List<String>> logAggregationDiagnosticsForNMs =
      new HashMap<>();
  // 存储每个NodeManager的日志聚合失败信息
  private Map<NodeId, List<String>> logAggregationFailureMessagesForNMs =
      new HashMap<>();
  private final int maxLogAggregationDiagnosticsInMemory;

  /**
   * 构造函数，从配置初始化日志聚合管理器。
   * @param conf YARN配置
   * @param readLock 共享读锁，来自RMApp
   * @param writeLock 独占写锁，来自RMApp
   */
  RMAppLogAggregation(Configuration conf, ReadLock readLock,
      WriteLock writeLock) {
    this.readLock = readLock;
    this.writeLock = writeLock;
    this.logAggregationStatusTimeout = getLogAggregationStatusTimeout(conf);
    this.logAggregationEnabled = getEnabledFlagFromConf(conf);
    this.logAggregationStatusForAppReport =
        this.logAggregationEnabled ? LogAggregationStatus.NOT_START :
            LogAggregationStatus.DISABLED;
    this.maxLogAggregationDiagnosticsInMemory =
        getMaxLogAggregationDiagnostics(conf);
  }

  /**
   * 从配置中获取日志聚合状态超时时间。
   * @param conf YARN配置
   * @return 超时时间，单位毫秒
   */
  private long getLogAggregationStatusTimeout(Configuration conf) {
    long statusTimeout =
        conf.getLong(YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
            YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS);
    if (statusTimeout <= 0) {
      return YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS;
    } else {
      return statusTimeout;
    }
  }

  /**
   * 从配置获取日志聚合是否启用的标志。
   * @param conf YARN配置
   * @return 是否启用日志聚合
   */
  private boolean getEnabledFlagFromConf(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
  }

  /**
   * 从配置获取内存中最多保存的日志聚合诊断信息条数。
   * @param conf YARN配置
   * @return 最大诊断信息条数
   */
  private int getMaxLogAggregationDiagnostics(Configuration conf) {
    return conf.getInt(
        YarnConfiguration.RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY,
        YarnConfiguration.DEFAULT_RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY);
  }

  /**
   * 获取应用所有节点的日志聚合报告，超时未完成的节点自动标记为超时。
   * @param rmApp 当前RM应用实例
   * @return 不可修改的节点->报告映射
   */
  Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp(
      RMAppImpl rmApp) {
    this.readLock.lock();
    try {
      // 应用已进入终态但日志聚合未完成，且已超时，将未完成节点标记为超时
      if (!isLogAggregationFinished() && RMAppImpl.isAppInFinalState(rmApp) &&
          rmApp.getSystemClock().getTime() > this.logAggregationStartTime
              + this.logAggregationStatusTimeout) {
        for (Map.Entry<NodeId, LogAggregationReport> output :
            logAggregationStatus.entrySet()) {
          if (!output.getValue().getLogAggregationStatus()
              .equals(LogAggregationStatus.TIME_OUT)
              && !output.getValue().getLogAggregationStatus()
              .equals(LogAggregationStatus.SUCCEEDED)
              && !output.getValue().getLogAggregationStatus()
              .equals(LogAggregationStatus.FAILED)) {
            output.getValue().setLogAggregationStatus(
                LogAggregationStatus.TIME_OUT);
          }
        }
      }
      return Collections.unmodifiableMap(logAggregationStatus);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 聚合来自NodeManager的最新日志聚合状态报告，更新本地状态。
   * @param nodeId 节点ID
   * @param report 最新的日志聚合报告
   * @param rmApp 当前RM应用实例
   */
  void aggregateLogReport(NodeId nodeId, LogAggregationReport report,
      RMAppImpl rmApp) {
    this.writeLock.lock();
    try {
      if (this.logAggregationEnabled && !isLogAggregationFinished()) {
        LogAggregationReport curReport = this.logAggregationStatus.get(nodeId);
        boolean stateChangedToFinal = false;
        // 新节点首次上报
        if (curReport == null) {
          this.logAggregationStatus.put(nodeId, report);
          if (isLogAggregationFinishedForNM(report)) {
            stateChangedToFinal = true;
          }
        } else {
          // 已有上报记录，判断是否刚进入终态
          if (isLogAggregationFinishedForNM(report)) {
            if (!isLogAggregationFinishedForNM(curReport)) {
              stateChangedToFinal = true;
            }
          }
          // 处理超时节点恢复运行的场景
          if (report.getLogAggregationStatus() != LogAggregationStatus.RUNNING
              || curReport.getLogAggregationStatus() !=
              LogAggregationStatus.RUNNING_WITH_FAILURE) {
            if (curReport.getLogAggregationStatus()
                == LogAggregationStatus.TIME_OUT
                && report.getLogAggregationStatus()
                == LogAggregationStatus.RUNNING) {
              // 当前节点之前被标记超时，现在重新上报运行状态，根据是否有失败消息设置正确状态
              if (isThereFailureMessageForNM(nodeId)) {
                report.setLogAggregationStatus(
                    LogAggregationStatus.RUNNING_WITH_FAILURE);
              }
            }
            curReport.setLogAggregationStatus(report
                .getLogAggregationStatus());
          }
        }
        // 更新诊断信息和失败消息
        updateLogAggregationDiagnosticMessages(nodeId, report);
        // 如果应用已终态且本节点刚完成聚合，更新应用整体聚合状态
        if (RMAppImpl.isAppInFinalState(rmApp) && stateChangedToFinal) {
          updateLogAggregationStatus(nodeId);
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 获取应用整体的日志聚合状态，用于对外汇报。
   * @param rmApp 当前RM应用实例
   * @return 应用整体日志聚合状态
   */
  public LogAggregationStatus getLogAggregationStatusForAppReport(
      RMAppImpl rmApp) {
    boolean appInFinalState = RMAppImpl.isAppInFinalState(rmApp);
    this.readLock.lock();
    try {
      if (!logAggregationEnabled) {
        return LogAggregationStatus.DISABLED;
      }
      if (isLogAggregationFinished()) {
        return this.logAggregationStatusForAppReport;
      }
      Map<NodeId, LogAggregationReport> reports =
          getLogAggregationReportsForApp(rmApp);
      if (reports.size() == 0) {
        return this.logAggregationStatusForAppReport;
      }
      // 统计各状态节点数量
      int logNotStartCount = 0;
      int logCompletedCount = 0;
      int logTimeOutCount = 0;
      int logFailedCount = 0;
      int logRunningWithFailure = 0;
      for (Map.Entry<NodeId, LogAggregationReport> report :
          reports.entrySet()) {
        switch (report.getValue().getLogAggregationStatus()) {
          case NOT_START:
            logNotStartCount++;
            break;
          case RUNNING_WITH_FAILURE:
            logRunningWithFailure ++;
            break;
          case SUCCEEDED:
            logCompletedCount++;
            break;
          case FAILED:
            logFailedCount++;
            logCompletedCount++;
            break;
          case TIME_OUT:
            logTimeOutCount++;
            logCompletedCount++;
            break;
          default:
            break;
        }
      }
      // 所有节点都未启动
      if (logNotStartCount == reports.size()) {
        return LogAggregationStatus.NOT_START;
      } else if (logCompletedCount == reports.size()) {
        // 所有节点都已完成，根据结果返回对应状态
        if (logFailedCount > 0 && appInFinalState) {
          this.logAggregationStatusForAppReport =
              LogAggregationStatus.FAILED;
          return LogAggregationStatus.FAILED;
        } else if (logTimeOutCount > 0) {
          this.logAggregationStatusForAppReport =
              LogAggregationStatus.TIME_OUT;
          return LogAggregationStatus.TIME_OUT;
        }
        if (appInFinalState) {
          this.logAggregationStatusForAppReport =
              LogAggregationStatus.SUCCEEDED;
          return LogAggregationStatus.SUCCEEDED;
        }
      } else if (logRunningWithFailure > 0) {
        return LogAggregationStatus.RUNNING_WITH_FAILURE;
      }
      return LogAggregationStatus.RUNNING;
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 检查应用整体日志聚合是否已完成。
   * @return 是否完成
   */
  private boolean isLogAggregationFinished() {
    return this.logAggregationStatusForAppReport
        .equals(LogAggregationStatus.SUCCEEDED)
        || this.logAggregationStatusForAppReport
        .equals(LogAggregationStatus.FAILED)
        || this.logAggregationStatusForAppReport
        .equals(LogAggregationStatus.TIME_OUT);

  }

  /**
   * 检查单个NodeManager节点的日志聚合是否已完成。
   * @param report 节点日志聚合报告
   * @return 是否完成
   */
  private boolean isLogAggregationFinishedForNM(LogAggregationReport report) {
    return report.getLogAggregationStatus() == LogAggregationStatus.SUCCEEDED
        || report.getLogAggregationStatus() == LogAggregationStatus.FAILED;
  }

  /**
   * 更新NodeManager上报的日志聚合诊断消息和失败消息。
   * @param nodeId 节点ID
   * @param report 最新日志聚合报告
   */
  private void updateLogAggregationDiagnosticMessages(NodeId nodeId,
      LogAggregationReport report) {
    if (report.getDiagnosticMessage() != null
        && !report.getDiagnosticMessage().isEmpty()) {
      if (report.getLogAggregationStatus()
          == LogAggregationStatus.RUNNING ) {
        // 运行中状态，保存普通诊断信息
        List<String> diagnostics = logAggregationDiagnosticsForNMs.get(nodeId);
        if (diagnostics == null) {
          diagnostics = new ArrayList<>();
          logAggregationDiagnosticsForNMs.put(nodeId, diagnostics);
        } else {
          // 超过最大保存条数，移除最早的一条
          if (diagnostics.size()
              == maxLogAggregationDiagnosticsInMemory) {
            diagnostics.remove(0);
          }
        }
        diagnostics.add(report.getDiagnosticMessage());
        // 更新报告中的拼接诊断信息
        this.logAggregationStatus.get(nodeId).setDiagnosticMessage(
            StringUtils.join(diagnostics, "\n"));
      } else if (report.getLogAggregationStatus()
          == LogAggregationStatus.RUNNING_WITH_FAILURE) {
        // 运行失败状态，保存失败消息
        List<String> failureMessages =
            logAggregationFailureMessagesForNMs.get(nodeId);
        if (failureMessages == null) {
          failureMessages = new ArrayList<>();
          logAggregationFailureMessagesForNMs.put(nodeId, failureMessages);
        } else {
          // 超过最大保存条数，移除最早的一条
          if (failureMessages.size()
              == maxLogAggregationDiagnosticsInMemory) {
            failureMessages.remove(0);
          }
        }
        failureMessages.add(report.getDiagnosticMessage());
      }
    }
  }

  /**
   * 节点完成聚合后，更新应用整体聚合状态，清理已完成节点缓存。
   * @param nodeId 刚完成聚合的节点ID
   */
  private void updateLogAggregationStatus(NodeId nodeId) {
    LogAggregationStatus status =
        this.logAggregationStatus.get(nodeId).getLogAggregationStatus();
    // 更新成功/失败计数
    if (status.equals(LogAggregationStatus.SUCCEEDED)) {
      this.logAggregationSucceed++;
    } else if (status.equals(LogAggregationStatus.FAILED)) {
      this.logAggregationFailed++;
    }
    // 所有节点聚合成功，清理所有缓存，标记整体成功
    if (this.logAggregationSucceed == this.logAggregationStatus.size()) {
      this.logAggregationStatusForAppReport =
          LogAggregationStatus.SUCCEEDED;
      this.logAggregationStatus.clear();
      this.logAggregationDiagnosticsForNMs.clear();
      this.logAggregationFailureMessagesForNMs.clear();
    } else if (this.logAggregationSucceed + this.logAggregationFailed
        == this.logAggregationStatus.size()) {
      // 所有节点都完成，但有失败节点，标记整体失败，清理成功节点缓存
      this.logAggregationStatusForAppReport = LogAggregationStatus.FAILED;
      this.logAggregationStatus.entrySet().removeIf(entry ->
          entry.getValue().getLogAggregationStatus()
          .equals(LogAggregationStatus.SUCCEEDED));
      this.logAggregationDiagnosticsForNMs.clear();
    }
  }

  /**
   * 获取指定NodeManager的日志聚合失败拼接消息。
   * @param nodeId 节点ID
   * @return 拼接后的失败消息，无消息返回空串
   */
  String getLogAggregationFailureMessagesForNM(NodeId nodeId) {
    this.readLock.lock();
    try {
      List<String> failureMessages =
          this.logAggregationFailureMessagesForNMs.get(nodeId);
      if (failureMessages == null || failureMessages.isEmpty()) {
        return StringUtils.EMPTY;
      }
      return StringUtils.join(failureMessages, "\n");
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 记录日志聚合开始时间。
   * @param time 开始时间戳
   */
  void recordLogAggregationStartTime(long time) {
    logAggregationStartTime = time;
  }

  /**
   * 获取日志聚合是否启用。
   * @return 是否启用
   */
  public boolean isEnabled() {
    return logAggregationEnabled;
  }

  /**
   * 检查指定NodeManager是否已有日志聚合报告。
   * @param nodeId 节点ID
   * @return 是否已有报告
   */
  private boolean hasReportForNodeManager(NodeId nodeId) {
    return logAggregationStatus.containsKey(nodeId);
  }

  /**
   * 添加指定NodeManager的日志聚合报告。