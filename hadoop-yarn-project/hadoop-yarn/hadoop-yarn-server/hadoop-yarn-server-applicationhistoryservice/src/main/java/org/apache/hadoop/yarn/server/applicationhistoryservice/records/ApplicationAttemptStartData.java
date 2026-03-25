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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

/**
 * 应用尝试启动数据记录，保存RMAppAttempt启动时确定的、需要持久化存储的信息，供应用历史服务使用。
 */
@Public
@Unstable
public abstract class ApplicationAttemptStartData {

  /**
   * 创建并初始化应用尝试启动数据实例，设置所有必填字段。
   * @param appAttemptId 应用尝试ID
   * @param host 应用尝试的监听主机地址
   * @param rpcPort 应用尝试的RPC服务端口
   * @param masterContainerId 应用尝试的AM容器ID
   * @return 初始化完成的应用尝试启动数据实例
   */
  @Public
  @Unstable
  public static ApplicationAttemptStartData newInstance(
      ApplicationAttemptId appAttemptId, String host, int rpcPort,
      ContainerId masterContainerId) {
    ApplicationAttemptStartData appAttemptSD =
        Records.newRecord(ApplicationAttemptStartData.class);
    appAttemptSD.setApplicationAttemptId(appAttemptId);
    appAttemptSD.setHost(host);
    appAttemptSD.setRPCPort(rpcPort);
    appAttemptSD.setMasterContainerId(masterContainerId);
    return appAttemptSD;
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
  public abstract String getHost();

  @Public
  @Unstable
  public abstract void setHost(String host);

  @Public
  @Unstable
  public abstract int getRPCPort();

  @Public
  @Unstable
  public abstract void setRPCPort(int rpcPort);

  @Public
  @Unstable
  public abstract ContainerId getMasterContainerId();

  @Public
  @Unstable
  public abstract void setMasterContainerId(ContainerId masterContainerId);

}