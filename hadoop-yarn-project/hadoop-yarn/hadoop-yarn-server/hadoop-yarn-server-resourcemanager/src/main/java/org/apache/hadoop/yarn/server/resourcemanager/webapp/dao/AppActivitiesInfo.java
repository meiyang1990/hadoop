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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AppAllocation;
import org.apache.hadoop.yarn.util.SystemClock;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * YARN ResourceManager Web UI 应用分配活动信息数据访问对象，用于封装应用调度分配活动信息供前端展示。
 */
@XmlRootElement(name = "appActivities")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppActivitiesInfo {
  // 应用ID
  private String applicationId;
  // 诊断信息，用于存储错误或状态提示
  private String diagnostic;
  // 时间戳（毫秒）
  private Long timestamp;
  // 格式化日期时间字符串
  private String dateTime;
  // 应用分配活动信息列表
  private List<AppAllocationInfo> allocations;

  // 日志实例
  private static final Logger LOG =
      LoggerFactory.getLogger(AppActivitiesInfo.class);

  /**
   * 默认构造函数，供JAXB反序列化使用。
   */
  public AppActivitiesInfo() {
  }

  /**
   * 构造错误场景下的应用活动信息对象。
   * @param errorMessage 错误信息
   * @param applicationId 应用ID
   */
  public AppActivitiesInfo(String errorMessage, String applicationId) {
    this.diagnostic = errorMessage;
    this.applicationId = applicationId;
    setTime(SystemClock.getInstance().getTime());
  }

  /**
   * 从应用分配列表构造完整的应用活动信息对象。
   * @param appAllocations 应用分配活动列表
   * @param applicationId 应用ID
   * @param groupBy 分组维度配置
   */
  public AppActivitiesInfo(List<AppAllocation> appAllocations,
      ApplicationId applicationId,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    this.applicationId = applicationId.toString();
    this.allocations = new ArrayList<>();

    if (appAllocations == null) {
      // 未查询到分配信息，提示等待
      diagnostic = "waiting for display";
      setTime(SystemClock.getInstance().getTime());
    } else {
      // 逆序遍历分配列表，将最新的活动排在前端展示的最前面
      for (int i = appAllocations.size() - 1; i > -1; i--) {
        AppAllocation appAllocation = appAllocations.get(i);
        AppAllocationInfo appAllocationInfo = new AppAllocationInfo(
            appAllocation, groupBy);
        this.allocations.add(appAllocationInfo);
      }
    }
  }

  /**
   * 设置时间戳和格式化日期字符串。
   * @param ts 毫秒时间戳
   */
  private void setTime(long ts) {
    this.timestamp = ts;
    this.dateTime = new Date(ts).toString();
  }

  @VisibleForTesting
  public List<AppAllocationInfo> getAllocations() {
    return allocations;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getDateTime() {
    return dateTime;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getDiagnostic() {
    return diagnostic;
  }
}