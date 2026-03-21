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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
        .CapacityScheduler;
import org.apache.hadoop.yarn.webapp.BadRequestException;

import java.io.IOException;
import java.util.Set;

import static org.apache.hadoop.yarn.server.webapp.WebServices.parseQueries;

/**
 * YARN RM Web API 获取应用列表请求构造器，采用Builder模式组装过滤条件，
 * 验证参数合法性后构造标准的GetApplicationsRequest对象。
 */
public class ApplicationsRequestBuilder {

  private Set<String> statesQuery = Sets.newHashSet();
  private Set<String> users = Sets.newHashSetWithExpectedSize(1);
  private Set<String> queues = Sets.newHashSetWithExpectedSize(1);
  private String limit = null;
  private Long limitNumber;

  // 起止时间未指定时设置默认值
  private long startedTimeBegin = 0;
  private long startedTimeEnd = Long.MAX_VALUE;
  private long finishTimeBegin = 0;
  private long finishTimeEnd = Long.MAX_VALUE;
  private Set<String> appTypes = Sets.newHashSet();
  private Set<String> appTags = Sets.newHashSet();
  private String name = null;
  private ResourceManager rm;

  private ApplicationsRequestBuilder() {
  }

  /**
   * 创建构造器实例，使用静态工厂方法。
   * @return 新的构造器实例
   */
  public static ApplicationsRequestBuilder create() {
    return new ApplicationsRequestBuilder();
  }

  /**
   * 添加应用状态过滤条件，兼容已废弃的单状态参数。
   * @param stateQuery 应用状态字符串
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withStateQuery(String stateQuery) {
    // stateQuery 已废弃，保留兼容旧版本请求
    if (stateQuery != null && !stateQuery.isEmpty()) {
      statesQuery.add(stateQuery);
    }
    return this;
  }

  /**
   * 添加批量应用状态过滤条件。
   * @param statesQuery 应用状态集合
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withStatesQuery(
      Set<String> statesQuery) {
    if (statesQuery != null) {
      this.statesQuery.addAll(statesQuery);
    }
    return this;
  }

  /**
   * 添加提交用户过滤条件。
   * @param userQuery 用户名
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withUserQuery(String userQuery) {
    if (userQuery != null && !userQuery.isEmpty()) {
      users.add(userQuery);
    }
    return this;
  }

  /**
   * 添加队列过滤条件，同时保存RM引用用于后续参数校验。
   * @param rm ResourceManager实例
   * @param queueQuery 队列名称
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withQueueQuery(ResourceManager rm,
      String queueQuery) {
    this.rm = rm;
    if (queueQuery != null && !queueQuery.isEmpty()) {
      queues.add(queueQuery);
    }
    return this;
  }

  /**
   * 添加返回结果数量限制。
   * @param limit 限制数量字符串
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withLimit(String limit) {
    if (limit != null && !limit.isEmpty()) {
      this.limit = limit;
    }
    return this;
  }

  /**
   * 添加应用启动时间起始过滤条件。
   * @param startedBegin 启动起始时间字符串
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withStartedTimeBegin(
      String startedBegin) {
    if (startedBegin != null && !startedBegin.isEmpty()) {
      startedTimeBegin = parseLongValue(startedBegin, "startedTimeBegin");
    }
    return this;
  }

  /**
   * 添加应用启动时间结束过滤条件。
   * @param startedEnd 启动结束时间字符串
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withStartedTimeEnd(String startedEnd) {
    if (startedEnd != null && !startedEnd.isEmpty()) {
      startedTimeEnd = parseLongValue(startedEnd, "startedTimeEnd");
    }
    return this;
  }

  /**
   * 添加应用完成时间起始过滤条件。
   * @param finishBegin 完成起始时间字符串
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withFinishTimeBegin(String finishBegin) {
    if (finishBegin != null && !finishBegin.isEmpty()) {
      finishTimeBegin = parseLongValue(finishBegin, "finishedTimeBegin");
    }
    return this;
  }

  /**
   * 添加应用完成时间结束过滤条件。
   * @param finishEnd 完成结束时间字符串
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withFinishTimeEnd(String finishEnd) {
    if (finishEnd != null && !finishEnd.isEmpty()) {
      finishTimeEnd = parseLongValue(finishEnd, "finishedTimeEnd");
    }
    return this;
  }

  /**
   * 添加应用类型过滤条件。
   * @param applicationTypes 应用类型集合
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withApplicationTypes(
      Set<String> applicationTypes) {
    if (applicationTypes !=  null) {
      appTypes = parseQueries(applicationTypes, false);
    }
    return this;
  }

  /**
   * 添加应用标签过滤条件。
   * @param applicationTags 应用标签集合
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withApplicationTags(
      Set<String> applicationTags) {
    if (applicationTags != null) {
      appTags = parseQueries(applicationTags, false);
    }
    return this;
  }

  /**
   * 添加应用名称过滤条件。
   * @param applicationName 应用名称
   * @return 当前构造器实例
   */
  public ApplicationsRequestBuilder withName(String applicationName) {
    name = applicationName;
    return this;
  }

  /**
   * 对所有请求参数执行统一合法性校验。
   */
  private void validate() {
    queues.forEach(q -> validateQueueExists(rm, q));
    validateLimit();
    validateStartTime();
    validateFinishTime();
  }

  /**
   * 验证指定队列在当前RM调度器中是否存在。
   * @param rm ResourceManager实例
   * @param queueQuery 待验证队列名称
   */
  private void validateQueueExists(ResourceManager rm, String queueQuery) {
    ResourceScheduler rs = rm.getResourceScheduler();
    // 仅对容量调度器执行队列存在性校验
    if (rs instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rs;
      try {
        // 通过获取队列信息判断队列是否存在
        cs.getQueueInfo(queueQuery, false, false);
      } catch (IOException e) {
        throw new BadRequestException(e.getMessage());
      }
    }
  }

  /**
   * 验证返回结果数量限制参数合法性。
   */
  private void validateLimit() {
    if (limit != null) {
      limitNumber = parseLongValue(limit, "limit");
      if (limitNumber <= 0) {
        throw new BadRequestException("limit value must be greater then 0");
      }
    }
  }

  /**
   * 将字符串参数解析为长整型，非法格式抛出请求错误。
   * @param strValue 待解析字符串
   * @param queryName 参数名称，用于错误提示
   * @return 解析后的长整型值
   */
  private long parseLongValue(String strValue, String queryName) {
    try {
      return Long.parseLong(strValue);
    } catch (NumberFormatException e) {
      throw new BadRequestException(queryName + " value must be a number!");
    }
  }

  /**
   * 验证启动时间范围参数合法性。
   */
  private void validateStartTime() {
    if (startedTimeBegin < 0) {
      throw new BadRequestException("startedTimeBegin must be greater than 0");
    }
    if (startedTimeEnd < 0) {
      throw new BadRequestException("startedTimeEnd must be greater than 0");
    }
    if (startedTimeBegin > startedTimeEnd) {
      throw new BadRequestException(
          "startedTimeEnd must be greater than startTimeBegin");
    }
  }

  /**
   * 验证完成时间范围参数合法性。
   */
  private void validateFinishTime() {
    if (finishTimeBegin < 0) {
      throw new BadRequestException("finishTimeBegin must be greater than 0");
    }
    if (finishTimeEnd < 0) {
      throw new BadRequestException("finishTimeEnd must be greater than 0");
    }
    if (finishTimeBegin > finishTimeEnd) {
      throw new BadRequestException(
          "finishTimeEnd must be greater than finishTimeBegin");
    }
  }

  /**
   * 完成参数校验，构造并返回标准的获取应用列表请求对象。
   * @return 填充好所有过滤条件的GetApplicationsRequest实例
   */
  public GetApplicationsRequest build() {
    validate();
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();

    Set<String> appStates = parseQueries(statesQuery, true);
    if (!appStates.isEmpty()) {
      request.setApplicationStates(appStates);
    }
    if (!users.isEmpty()) {
      request.setUsers(users);
    }
    if (!queues.isEmpty()) {
      request.setQueues(queues);
    }
    if (limitNumber != null) {
      request.setLimit(limitNumber);
    }
    request.setStartRange(startedTimeBegin, startedTimeEnd);
    request.setFinishRange(finishTimeBegin, finishTimeEnd);

    if (!appTypes.isEmpty()) {
      request.setApplicationTypes(appTypes);
    }
    if (!appTags.isEmpty()) {
      request.setApplicationTags(appTags);
    }
    if (name != null) {
      request.setName(name);
    }

    return request;
  }
}