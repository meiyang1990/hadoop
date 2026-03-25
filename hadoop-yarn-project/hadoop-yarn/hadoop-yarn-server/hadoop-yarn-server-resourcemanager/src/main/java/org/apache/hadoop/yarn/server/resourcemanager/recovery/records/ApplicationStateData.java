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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.Records;

/**
 * 应用持久化恢复状态数据，存储ResourceManager重启恢复所需的全部应用状态信息
 */
@Public
@Unstable
public abstract class ApplicationStateData {
  // 存储应用所有尝试的状态数据，按尝试ID索引
  public Map<ApplicationAttemptId, ApplicationAttemptStateData> attempts =
      new HashMap<ApplicationAttemptId, ApplicationAttemptStateData>();
  
  /**
   * 创建应用状态数据实例
   * @param submitTime 应用提交时间
   * @param startTime 应用启动时间
   * @param user 提交应用的用户名
   * @param submissionContext 应用提交上下文
   * @param state 应用当前状态
   * @param diagnostics 诊断信息
   * @param launchTime 应用启动时间
   * @param finishTime 应用完成时间
   * @param callerContext 调用上下文信息
   * @return 新建的应用状态数据实例
   */
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, String user,
      ApplicationSubmissionContext submissionContext, RMAppState state,
      String diagnostics, long launchTime, long finishTime,
      CallerContext callerContext) {
    ApplicationStateData appState = Records.newRecord(ApplicationStateData.class);
    appState.setSubmitTime(submitTime);
    appState.setStartTime(startTime);
    appState.setUser(user);
    appState.setApplicationSubmissionContext(submissionContext);
    appState.setState(state);
    appState.setDiagnostics(diagnostics);
    appState.setLaunchTime(launchTime);
    appState.setFinishTime(finishTime);
    appState.setCallerContext(callerContext);
    return appState;
  }

  /**
   * 创建带应用超时配置的应用状态数据实例
   * @param submitTime 应用提交时间
   * @param startTime 应用启动时间
   * @param user 提交应用的用户名
   * @param submissionContext 应用提交上下文
   * @param state 应用当前状态
   * @param diagnostics 诊断信息
   * @param launchTime 应用启动时间
   * @param finishTime 应用完成时间
   * @param callerContext 调用上下文信息
   * @param applicationTimeouts 应用各类超时配置
   * @return 新建的应用状态数据实例
   */
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, String user,
      ApplicationSubmissionContext submissionContext, RMAppState state,
      String diagnostics, long launchTime, long finishTime,
      CallerContext callerContext,
      Map<ApplicationTimeoutType, Long> applicationTimeouts) {
    ApplicationStateData appState =
        Records.newRecord(ApplicationStateData.class);
    appState.setSubmitTime(submitTime);
    appState.setStartTime(startTime);
    appState.setUser(user);
    appState.setApplicationSubmissionContext(submissionContext);
    appState.setState(state);
    appState.setDiagnostics(diagnostics);
    appState.setLaunchTime(launchTime);
    appState.setFinishTime(finishTime);
    appState.setCallerContext(callerContext);
    appState.setApplicationTimeouts(applicationTimeouts);
    return appState;
  }

  /**
   * 创建新提交应用的状态数据实例
   * @param submitTime 应用提交时间
   * @param startTime 应用启动时间
   * @param context 应用提交上下文
   * @param user 提交应用的用户名
   * @param callerContext 调用上下文信息
   * @return 新建的应用状态数据实例
   */
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, ApplicationSubmissionContext context, String user,
      CallerContext callerContext) {
    return newInstance(submitTime, startTime, user, context, null, "", 0, 0,
        callerContext);
  }
  
  /**
   * 创建新提交应用的状态数据实例，无调用上下文
   * @param submitTime 应用提交时间
   * @param startTime 应用启动时间
   * @param context 应用提交上下文
   * @param user 提交应用的用户名
   * @return 新建的应用状态数据实例
   */
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, ApplicationSubmissionContext context, String user) {
    return newInstance(submitTime, startTime, context, user,
        (CallerContext) null);
  }

  /**
   * 创建带真实用户信息的应用状态数据实例
   * @param submitTime 应用提交时间
   * @param startTime 应用启动时间
   * @param user 提交应用的用户名
   * @param realUser 代理运行的真实用户名
   * @param submissionContext 应用提交上下文
   * @param state 应用当前状态
   * @param diagnostics 诊断信息
   * @param launchTime 应用启动时间
   * @param finishTime 应用完成时间
   * @param callerContext 调用上下文信息
   * @return 新建的应用状态数据实例
   */
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, String user, String realUser,
      ApplicationSubmissionContext submissionContext, RMAppState state,
      String diagnostics, long launchTime, long finishTime,
      CallerContext callerContext) {
    ApplicationStateData appState =
        newInstance(submitTime, startTime, user, submissionContext, state,
            diagnostics, launchTime, finishTime, callerContext);
    if (realUser != null) {
      appState.setRealUser(realUser);
    }
    return appState;
  }

  /**
   * 创建带真实用户信息和超时配置的应用状态数据实例
   * @param submitTime 应用提交时间
   * @param startTime 应用启动时间
   * @param user 提交应用的用户名
   * @param realUser 代理运行的真实用户名
   * @param submissionContext 应用提交上下文
   * @param state 应用当前状态
   * @param diagnostics 诊断信息
   * @param launchTime 应用启动时间
   * @param finishTime 应用完成时间
   * @param callerContext 调用上下文信息
   * @param applicationTimeouts 应用各类超时配置
   * @return 新建的应用状态数据实例
   */
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, String user, String realUser,
      ApplicationSubmissionContext submissionContext, RMAppState state,
      String diagnostics, long launchTime, long finishTime,
      CallerContext callerContext,
      Map<ApplicationTimeoutType, Long> applicationTimeouts) {
    ApplicationStateData appState =
        newInstance(submitTime, startTime, user, submissionContext, state,
            diagnostics, launchTime, finishTime, callerContext, applicationTimeouts);
    if (realUser != null) {
      appState.setRealUser(realUser);
    }
    return appState;
  }

  /**
   * 创建带真实用户信息的新提交应用状态数据实例
   * @param submitTime 应用提交时间
   * @param startTime 应用启动时间
   * @param context 应用提交上下文
   * @param user 提交应用的用户名
   * @param realUser 代理运行的真实用户名
   * @param callerContext 调用上下文信息
   * @return 新建的应用状态数据实例
   */
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, ApplicationSubmissionContext context, String user,
      String realUser, CallerContext callerContext) {
    return newInstance(submitTime, startTime, user, realUser, context, null, "",
        0, 0, callerContext);
  }

  /**
   * 获取应用已有的尝试次数
   * @return 应用尝试数量
   */
  public int getAttemptCount() {
    return attempts.size();
  }

  /**
   * 根据尝试ID获取对应尝试的状态数据
   * @param attemptId 应用尝试ID
   * @return 对应尝试的状态数据
   */
  public ApplicationAttemptStateData getAttempt(
      ApplicationAttemptId  attemptId) {
    return attempts.get(attemptId);
  }

  /**
   * 获取应用第一个尝试的尝试ID序号
   * @return 最小尝试ID序号，无尝试时返回默认值1
   */
  public int getFirstAttemptId() {
    int min = Integer.MAX_VALUE;
    // 遍历所有尝试找到最小的尝试ID序号
    for(ApplicationAttemptId attemptId : attempts.keySet()) {
      if (attemptId.getAttemptId() < min) {
        min = attemptId.getAttemptId();
      }
    }
    return min == Integer.MAX_VALUE ? 1 : min;
  }

  /**
   * 获取对应Proto序列化对象
   * @return 应用状态数据的Proto序列化对象
   */
  public abstract ApplicationStateDataProto getProto();

  /**
   * 获取ResourceManager接收应用的时间
   * @return 应用提交时间
   */
  @Public
  @Unstable
  public abstract long getSubmitTime();
  
  @Public
  @Unstable
  public abstract void setSubmitTime(long submitTime);

  /**
   * 获取应用启动时间
   * @return 应用启动时间
   */
  @Public
  @Stable
  public abstract long getStartTime();

  @Private
  @Unstable
  public abstract void setStartTime(long startTime);


  /**
   * 获取应用容器启动时间
   * @return 应用启动时间
   */
  @Public
  @Stable
  public abstract long getLaunchTime();

  @Private
  @Unstable
  public abstract void setLaunchTime(long launchTime);

  /**
   * 设置应用提交用户名
   * @param user 提交者用户名
   */
  @Public
  @Unstable
  public abstract void setUser(String user);
  
  @Public
  @Unstable
  public abstract String getUser();
  
  /**
   * 获取应用提交上下文，包含应用ID等核心信息
   * @return 应用提交上下文
   */
  @Public
  @Unstable
  public abstract ApplicationSubmissionContext getApplicationSubmissionContext();
  
  @Public
  @Unstable
  public abstract void setApplicationSubmissionContext(
      ApplicationSubmissionContext context);

  /**
   * 获取应用最终状态
   * @return 应用状态
   */
  public abstract RMAppState getState();

  public abstract void setState(RMAppState state);

  /**
   * 获取应用诊断信息
   * @return 诊断信息字符串
   */
  public abstract String getDiagnostics();

  public abstract void setDiagnostics(String diagnostics);

  /**
   * 获取应用完成时间
   * @return 应用完成时间
   */
  public abstract long getFinishTime();

  public abstract void setFinishTime(long finishTime);
  
  public abstract CallerContext getCallerContext();
  
  public abstract void setCallerContext(CallerContext callerContext);

  @Public
  public abstract Map<ApplicationTimeoutType, Long> getApplicationTimeouts();

  @Public
  public abstract void setApplicationTimeouts(
      Map<ApplicationTimeoutType, Long> applicationTimeouts);

  public abstract String getRealUser();

  public abstract void setRealUser(String realUser);
}