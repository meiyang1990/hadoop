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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;

/**
 * 应用程序结束事件写入应用历史的事件类。
 * 封装应用程序结束相关信息，用于资源管理器异步写入应用历史服务。
 */
public class WritingApplicationFinishEvent extends
    WritingApplicationHistoryEvent {

  private ApplicationId appId;
  private ApplicationFinishData appFinish;

  /**
   * 构造应用结束写入历史事件对象。
   * @param appId 应用程序ID
   * @param appFinish 应用程序结束信息数据
   */
  public WritingApplicationFinishEvent(ApplicationId appId,
      ApplicationFinishData appFinish) {
    super(WritingHistoryEventType.APP_FINISH);
    this.appId = appId;
    this.appFinish = appFinish;
  }

  @Override
  public int hashCode() {
    return appId.hashCode();
  }

  /**
   * 获取应用程序ID。
   * @return 应用程序ID
   */
  public ApplicationId getApplicationId() {
    return appId;
  }

  /**
   * 获取应用程序结束信息数据。
   * @return 应用程序结束数据，包含结束时间、最终状态等信息
   */
  public ApplicationFinishData getApplicationFinishData() {
    return appFinish;
  }

}