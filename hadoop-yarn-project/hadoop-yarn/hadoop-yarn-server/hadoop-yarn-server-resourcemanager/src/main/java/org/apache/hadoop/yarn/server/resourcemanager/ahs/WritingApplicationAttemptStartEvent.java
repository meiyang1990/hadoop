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

package org.apache.hadoop.yarn.server.resourcemanager.ahs;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;

/**
 * 应用尝试启动事件写入事件，用于RM向应用历史服务(AHS)传递应用尝试启动信息。
 * 继承自WritingApplicationHistoryEvent，封装应用尝试启动的相关数据。
 */
public class WritingApplicationAttemptStartEvent extends
    WritingApplicationHistoryEvent {

  // 应用尝试ID
  private ApplicationAttemptId appAttemptId;
  // 应用尝试启动数据
  private ApplicationAttemptStartData appAttemptStart;

  /**
   * 构造应用尝试启动写入事件。
   * @param appAttemptId 应用尝试ID
   * @param appAttemptStart 应用尝试启动数据
   */
  public WritingApplicationAttemptStartEvent(ApplicationAttemptId appAttemptId,
      ApplicationAttemptStartData appAttemptStart) {
    super(WritingHistoryEventType.APP_ATTEMPT_START);
    this.appAttemptId = appAttemptId;
    this.appAttemptStart = appAttemptStart;
  }

  @Override
  public int hashCode() {
    // 基于所属应用ID计算哈希值
    return appAttemptId.getApplicationId().hashCode();
  }

  /**
   * 获取应用尝试ID。
   * @return 应用尝试ID
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return appAttemptId;
  }

  /**
   * 获取应用尝试启动数据。
   * @return 应用尝试启动数据
   */
  public ApplicationAttemptStartData getApplicationAttemptStartData() {
    return appAttemptStart;
  }

}