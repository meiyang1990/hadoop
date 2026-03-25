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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;

/**
 * 应用初始化事件，NodeManager容器管理器中触发应用启动的事件，
 * 携带应用权限配置和日志聚合上下文信息。
 */
public class ApplicationInitEvent extends ApplicationEvent {

  // 应用访问权限配置表，键为访问类型，值为授权用户/组
  private final Map<ApplicationAccessType, String> applicationACLs;
  // 日志聚合上下文，包含日志聚合相关配置
  private final LogAggregationContext logAggregationContext;  

  /**
   * 构造应用初始化事件，不指定日志聚合上下文
   * @param appId 应用ID
   * @param acls 应用访问权限配置
   */
  public ApplicationInitEvent(ApplicationId appId,
      Map<ApplicationAccessType, String> acls) {
    this(appId, acls, null);
  }

  /**
   * 构造应用初始化事件，指定完整配置
   * @param appId 应用ID
   * @param acls 应用访问权限配置
   * @param logAggregationContext 日志聚合上下文
   */
  public ApplicationInitEvent(ApplicationId appId,
      Map<ApplicationAccessType, String> acls,
      LogAggregationContext logAggregationContext) {
    super(appId, ApplicationEventType.INIT_APPLICATION);
    this.applicationACLs = acls;
    this.logAggregationContext = logAggregationContext;
  }

  /**
   * 获取应用访问权限配置
   * @return 应用访问权限配置表
   */
  public Map<ApplicationAccessType, String> getApplicationACLs() {
    return this.applicationACLs;
  }

  /**
   * 获取日志聚合上下文
   * @return 日志聚合上下文配置
   */
  public LogAggregationContext getLogAggregationContext() {
    return this.logAggregationContext;
  }
}