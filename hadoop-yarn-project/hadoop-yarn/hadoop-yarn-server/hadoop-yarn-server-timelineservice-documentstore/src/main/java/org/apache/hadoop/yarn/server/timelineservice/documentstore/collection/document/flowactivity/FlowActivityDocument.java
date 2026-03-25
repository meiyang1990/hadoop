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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity;


import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 流活动文档，存储YARN流运行的活动摘要信息，用于时间线服务文档存储，仅保存有限信息用于展示所有流运行。
 */
public class FlowActivityDocument implements
    TimelineDocument<FlowActivityDocument> {

  private String id;
  // 固定文档类型为YARN流活动
  private final String type = TimelineEntityType.YARN_FLOW_ACTIVITY.toString();
  // 存储当日所有流活动子文档集合
  private Set<FlowActivitySubDoc> flowActivities = new HashSet<>();
  // 日期时间戳（秒级），用于按天分区存储
  private long dayTimestamp;
  // 流所属用户
  private String user;
  // 流名称
  private String flowName;

  public FlowActivityDocument() {
  }

  /**
   * 构造函数，添加单个流运行活动。
   * @param flowName 流名称
   * @param flowVersion 流版本
   * @param flowRunId 流运行ID
   */
  public FlowActivityDocument(String flowName, String flowVersion,
      long flowRunId) {
    flowActivities.add(new FlowActivitySubDoc(flowName,
        flowVersion, flowRunId));
  }

  /**
   * 合并传入的流活动文档到当前文档，用于 upsert 更新场景。
   * @param flowActivityDocument 需要合并的流活动文档
   */
  @Override
  public void merge(FlowActivityDocument flowActivityDocument) {
    if (flowActivityDocument.getDayTimestamp() > 0) {
      this.dayTimestamp = flowActivityDocument.getDayTimestamp();
    }
    this.flowName = flowActivityDocument.getFlowName();
    this.user = flowActivityDocument.getUser();
    this.id = flowActivityDocument.getId();
    this.flowActivities.addAll(flowActivityDocument.getFlowActivities());
  }

  @Override
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String getType() {
    return type;
  }

  /**
   * 添加一个新的流运行活动到当前文档。
   * @param flowActivityName 流活动名称
   * @param flowVersion 流版本
   * @param flowRunId 流运行ID
   */
  public void addFlowActivity(String flowActivityName, String flowVersion,
      long flowRunId) {
    flowActivities.add(new FlowActivitySubDoc(flowActivityName,
        flowVersion, flowRunId));
  }

  public Set<FlowActivitySubDoc> getFlowActivities() {
    return flowActivities;
  }

  public void setFlowActivities(Set<FlowActivitySubDoc> flowActivities) {
    this.flowActivities = flowActivities;
  }

  @Override
  public long getCreatedTime() {
    // 将秒级日期时间戳转换为毫秒级返回
    return TimeUnit.SECONDS.toMillis(dayTimestamp);
  }

  @Override
  public void setCreatedTime(long time) {
  }

  public long getDayTimestamp() {
    return dayTimestamp;
  }

  public void setDayTimestamp(long dayTimestamp) {
    this.dayTimestamp = dayTimestamp;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getFlowName() {
    return flowName;
  }

  public void setFlowName(String flowName) {
    this.flowName = flowName;
  }
}