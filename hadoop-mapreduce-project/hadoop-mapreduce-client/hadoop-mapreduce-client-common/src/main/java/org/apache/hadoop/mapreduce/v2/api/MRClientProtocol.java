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

package org.apache.hadoop.mapreduce.v2.api;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenResponse;

/**
 * MapReduce客户端与服务端通信的协议接口
 * 定义了客户端对MapReduce作业、任务进行管理操作的所有RPC方法，包括作业查询、杀作业、令牌管理等核心能力
 */
public interface MRClientProtocol {
  /**
   * 获取当前客户端连接的服务端地址
   * @return 当前连接的服务端网络地址
   */
  public InetSocketAddress getConnectAddress();

  /**
   * 获取作业运行报告，包含作业状态、进度、运行时间等核心信息
   * @param request 获取作业报告请求，包含作业ID
   * @return 作业运行报告响应
   * @throws IOException RPC调用或IO异常
   */
  public GetJobReportResponse getJobReport(GetJobReportRequest request) throws IOException;

  /**
   * 获取单个任务运行报告，包含任务状态、进度等信息
   * @param request 获取任务报告请求，包含任务ID
   * @return 任务运行报告响应
   * @throws IOException RPC调用或IO异常
   */
  public GetTaskReportResponse getTaskReport(GetTaskReportRequest request) throws IOException;

  /**
   * 获取单个任务尝试运行报告，包含任务尝试状态、进度等信息
   * @param request 获取任务尝试报告请求，包含任务尝试ID
   * @return 任务尝试运行报告响应
   * @throws IOException RPC调用或IO异常
   */
  public GetTaskAttemptReportResponse getTaskAttemptReport(GetTaskAttemptReportRequest request) throws IOException;

  /**
   * 获取作业/任务的计数器信息
   * @param request 获取计数器请求，包含目标作业/任务ID
   * @return 计数器信息响应
   * @throws IOException RPC调用或IO异常
   */
  public GetCountersResponse getCounters(GetCountersRequest request) throws IOException;

  /**
   * 获取任务尝试完成事件列表，用于查询任务尝试完成情况
   * @param request 获取完成事件请求，包含作业ID、范围区间
   * @return 任务尝试完成事件响应
   * @throws IOException RPC调用或IO异常
   */
  public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(GetTaskAttemptCompletionEventsRequest request) throws IOException;

  /**
   * 获取指定类型所有任务的报告列表
   * @param request 获取任务报告列表请求，包含作业ID和任务类型
   * @return 所有匹配任务的报告响应
   * @throws IOException RPC调用或IO异常
   */
  public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request) throws IOException;

  /**
   * 获取任务尝试的诊断日志信息
   * @param request 获取诊断信息请求，包含任务尝试ID
   * @return 诊断日志响应
   * @throws IOException RPC调用或IO异常
   */
  public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request) throws IOException;

  /**
   * 杀死指定作业
   * @param request 杀作业请求，包含作业ID
   * @return 杀作业操作响应
   * @throws IOException RPC调用或IO异常
   */
  public KillJobResponse killJob(KillJobRequest request) throws IOException;

  /**
   * 杀死指定任务
   * @param request 杀任务请求，包含任务ID
   * @return 杀任务操作响应
   * @throws IOException RPC调用或IO异常
   */
  public KillTaskResponse killTask(KillTaskRequest request) throws IOException;

  /**
   * 杀死指定任务尝试
   * @param request 杀任务尝试请求，包含任务尝试ID
   * @return 杀任务尝试操作响应
   * @throws IOException RPC调用或IO异常
   */
  public KillTaskAttemptResponse killTaskAttempt(KillTaskAttemptRequest request) throws IOException;

  /**
   * 将指定任务尝试标记为失败
   * @param request 标记任务尝试失败请求，包含任务尝试ID
   * @return 标记操作响应
   * @throws IOException RPC调用或IO异常
   */
  public FailTaskAttemptResponse failTaskAttempt(FailTaskAttemptRequest request) throws IOException;

  /**
   * 获取委托令牌，用于身份认证
   * @param request 获取委托令牌请求
   * @return 包含委托令牌的响应
   * @throws IOException RPC调用或IO异常
   */
  public GetDelegationTokenResponse getDelegationToken(GetDelegationTokenRequest request) throws IOException;
  
  /**
   * Renew an existing delegation token.
   * 
   * @param request the delegation token to be renewed.
   * @return the new expiry time for the delegation token.
   * @throws IOException
   */
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws IOException;

  /**
   * Cancel an existing delegation token.
   * 
   * @param request the delegation token to be cancelled.
   * @return an empty response.
   * @throws IOException
   */
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws IOException;
}