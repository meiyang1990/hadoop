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

package org.apache.hadoop.yarn.server.router.clientrm;

import java.io.IOException;

import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.router.RouterAuditLogger;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;

import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SUBMIT_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.TARGET_CLIENT_RM_SERVICE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UNKNOWN;

/**
 * YARN Router应用提交上下文拦截器，用于防止DoS攻击。
 * 核心职责是在应用提交前检查ApplicationSubmissionContext的大小，
 * 避免过大的上下文导致Zookeeper故障，保障集群稳定性。
 */
public class ApplicationSubmissionContextInterceptor extends PassThroughClientRequestInterceptor {

  /**
   * 拦截处理应用提交请求，对提交上下文进行合法性和大小校验。
   * @param request 应用提交请求
   * @return 应用提交响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {

    // 校验请求基本信息完整性，检查必填字段是否为空
    if (request == null || request.getApplicationSubmissionContext() == null ||
        request.getApplicationSubmissionContext().getApplicationId() == null) {
      // 统计提交失败的应用
      RouterMetrics.getMetrics().incrAppsFailedSubmitted();
      String errMsg =
          "Missing submitApplication request or applicationSubmissionContext information.";
      // 记录审计日志
      RouterAuditLogger.logFailure(user.getShortUserName(), SUBMIT_NEW_APP, UNKNOWN,
          TARGET_CLIENT_RM_SERVICE, errMsg);
      // 记录错误日志并抛出异常
      RouterServerUtil.logAndThrowException(errMsg, null);
    }

    // 获取应用提交上下文，并转换为PB实现类
    ApplicationSubmissionContext appContext = request.getApplicationSubmissionContext();
    ApplicationSubmissionContextPBImpl asc = (ApplicationSubmissionContextPBImpl) appContext;

    // 检查提交上下文字段是否过大，超出限制则抛出异常
    RouterServerUtil.checkAppSubmissionContext(asc, getConf());

    // 校验通过，将请求传递给下一个拦截器继续处理
    return getNextInterceptor().submitApplication(request);
  }
}