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
package org.apache.hadoop.yarn.server.router.webapp.cache;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Set;

/**
 * Router应用信息缓存键类，封装Web应用查询应用列表的所有查询条件，
 * 作为缓存键使用，用于根据查询条件匹配缓存结果，提升Router查询性能。
 */
public class RouterAppInfoCacheKey {

  private static String user = "YarnRouter";

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAppInfoCacheKey.class.getName());

  // 当前请求用户凭证信息
  private UserGroupInformation ugi;
  // 应用状态查询参数
  private String stateQuery;
  // 应用状态集合查询参数
  private Set<String> statesQuery;
  // 最终状态查询参数
  private String finalStatusQuery;
  // 提交用户查询参数
  private String userQuery;
  // 队列查询参数
  private String queueQuery;
  // 返回数量查询参数
  private String count;
  // 启动时间起始查询参数
  private String startedBegin;
  // 启动时间结束查询参数
  private String startedEnd;
  // 完成时间起始查询参数
  private String finishBegin;
  // 完成时间结束查询参数
  private String finishEnd;
  // 应用类型集合查询参数
  private Set<String> applicationTypes;
  // 应用标签集合查询参数
  private Set<String> applicationTags;
  // 应用名称查询参数
  private String name;
  // 不返回的字段集合
  private Set<String> unselectedFields;

  public RouterAppInfoCacheKey() {

  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  /**
   * 全参数构造函数，创建包含所有查询条件的缓存键对象。
   * @param ugi 请求用户凭证
   * @param stateQuery 应用状态查询参数
   * @param statesQuery 应用状态集合查询参数
   * @param finalStatusQuery 最终状态查询参数
   * @param userQuery 提交用户查询参数
   * @param queueQuery 队列查询参数
   * @param count 返回数量查询参数
   * @param startedBegin 启动时间起始查询参数
   * @param startedEnd 启动时间结束查询参数
   * @param finishBegin 完成时间起始查询参数
   * @param finishEnd 完成时间结束查询参数
   * @param applicationTypes 应用类型集合查询参数
   * @param applicationTags 应用标签集合查询参数
   * @param name 应用名称查询参数
   * @param unselectedFields 不返回的字段集合
   */
  public RouterAppInfoCacheKey(UserGroupInformation ugi, String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery,
      String queueQuery, String count, String startedBegin, String startedEnd,
      String finishBegin, String finishEnd, Set<String> applicationTypes,
      Set<String> applicationTags, String name, Set<String> unselectedFields) {
    this.ugi = ugi;
    this.stateQuery = stateQuery;
    this.statesQuery = statesQuery;
    this.finalStatusQuery = finalStatusQuery;
    this.userQuery = userQuery;
    this.queueQuery = queueQuery;
    this.count = count;
    this.startedBegin = startedBegin;
    this.startedEnd = startedEnd;
    this.finishBegin = finishBegin;
    this.finishEnd = finishEnd;
    this.applicationTypes = applicationTypes;
    this.applicationTags = applicationTags;
    this.name = name;
    this.unselectedFields = unselectedFields;
  }


  @SuppressWarnings("checkstyle:ParameterNumber")
  /**
   * 根据HTTP请求和查询条件创建缓存键实例。
   * @param hsr HTTP请求对象
   * @param stateQuery 应用状态查询参数
   * @param statesQuery 应用状态集合查询参数
   * @param finalStatusQuery 最终状态查询参数
   * @param userQuery 提交用户查询参数
   * @param queueQuery 队列查询参数
   * @param count 返回数量查询参数
   * @param startedBegin 启动时间起始查询参数
   * @param startedEnd 启动时间结束查询参数
   * @param finishBegin 完成时间起始查询参数
   * @param finishEnd 完成时间结束查询参数
   * @param applicationTypes 应用类型集合查询参数
   * @param applicationTags 应用标签集合查询参数
   * @param name 应用名称查询参数
   * @param unselectedFields 不返回的字段集合
   * @return 创建好的缓存键实例，获取用户凭证失败返回null
   */
  public static RouterAppInfoCacheKey newInstance(HttpServletRequest hsr, String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery,
      String queueQuery, String count, String startedBegin, String startedEnd,
      String finishBegin, String finishEnd, Set<String> applicationTypes,
      Set<String> applicationTags, String name, Set<String> unselectedFields)  {

    UserGroupInformation callerUGI = null;
    // 从HTTP请求中获取调用用户凭证
    if (hsr != null) {
      callerUGI = RMWebAppUtil.getCallerUserGroupInformation(hsr, true);
    } else {
      // 请求为空时使用默认YarnRouter用户
      callerUGI = UserGroupInformation.createRemoteUser("YarnRouter");
    }

    // 用户凭证获取失败，记录错误返回空
    if (callerUGI == null) {
      LOG.error("Unable to obtain user name, user not authenticated.");
      return null;
    }

    return new RouterAppInfoCacheKey(
        callerUGI, stateQuery, statesQuery, finalStatusQuery, userQuery,
        queueQuery, count, startedBegin, startedEnd, finishBegin, finishEnd,
        applicationTypes, applicationTags, name, unselectedFields);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RouterAppInfoCacheKey that = (RouterAppInfoCacheKey) o;

    // 比较所有查询条件，仅比较用户名不比较UGI对象本身
    return new EqualsBuilder()
        .append(this.ugi.getUserName(), that.ugi.getUserName())
        .append(this.stateQuery, that.stateQuery)
        .append(this.statesQuery, that.statesQuery)
        .append(this.finalStatusQuery, that.finalStatusQuery)
        .append(this.userQuery, that.userQuery)
        .append(this.queueQuery, that.queueQuery)
        .append(this.count, that.count)
        .append(this.startedBegin, that.startedBegin)
        .append(this.startedEnd, that.startedEnd)
        .append(this.finishBegin, that.finishBegin)
        .append(this.finishEnd, that.finishEnd)
        .append(this.applicationTypes, that.applicationTypes)
        .append(this.applicationTags, that.applicationTags)
        .append(this.name, that.name)
        .append(this.unselectedFields, that.unselectedFields)
        .isEquals();
  }

  @Override
  public int hashCode() {
    // 基于所有查询条件计算哈希值，仅使用用户名
    return new HashCodeBuilder()
       .append(this.ugi.getUserName())
       .append(this.stateQuery)
       .append(this.statesQuery)
       .append(this.finalStatusQuery)
       .append(this.userQuery)
       .append(this.queueQuery)
       .append(this.count)
       .append(this.startedBegin)
       .append(this.startedEnd)
       .append(this.finishBegin)
       .append(this.finishEnd)
       .append(this.applicationTypes)
       .append(this.applicationTags)
       .append(this.name)
       .append(this.unselectedFields)
       .toHashCode();
  }
}