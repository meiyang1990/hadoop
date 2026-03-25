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

package org.apache.hadoop.mapreduce.jobhistory;

import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 作业历史事件，用于记录MapReduce应用Master(AM)启动事件
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AMStartedEvent implements HistoryEvent {
  private AMStarted datum = new AMStarted();
  private String forcedJobStateOnShutDown;
  private long submitTime;

  /**
   * 构造AM启动事件，用于记录MR AppMaster的启动信息
   * 
   * @param appAttemptId
   *          应用尝试ID
   * @param startTime
   *          AM启动时间
   * @param containerId
   *          AM运行所在容器ID
   * @param nodeManagerHost
   *          AM运行所在NodeManager主机地址
   * @param nodeManagerPort
   *          AM运行所在NodeManager服务端口
   * @param nodeManagerHttpPort
   *          AM运行所在NodeManager HTTP服务端口
   * @param submitTime
   *          作业提交时间
   */
  public AMStartedEvent(ApplicationAttemptId appAttemptId, long startTime,
      ContainerId containerId, String nodeManagerHost, int nodeManagerPort,
      int nodeManagerHttpPort, long submitTime) {
    this(appAttemptId, startTime, containerId, nodeManagerHost,
        nodeManagerPort, nodeManagerHttpPort, null, submitTime);
  }

  /**
   * 构造AM启动事件，可指定关闭时强制作业状态，用于记录MR AppMaster的启动信息
   *
   * @param appAttemptId
   *          应用尝试ID
   * @param startTime
   *          AM启动时间
   * @param containerId
   *          AM运行所在容器ID
   * @param nodeManagerHost
   *          AM运行所在NodeManager主机地址
   * @param nodeManagerPort
   *          AM运行所在NodeManager服务端口
   * @param nodeManagerHttpPort
   *          AM运行所在NodeManager HTTP服务端口
   * @param forcedJobStateOnShutDown
   *          AM关闭时强制作业设置的状态
   * @param submitTime
   *          作业提交时间
   */
  public AMStartedEvent(ApplicationAttemptId appAttemptId, long startTime,
      ContainerId containerId, String nodeManagerHost, int nodeManagerPort,
      int nodeManagerHttpPort, String forcedJobStateOnShutDown,
      long submitTime) {
    datum.setApplicationAttemptId(new Utf8(appAttemptId.toString()));
    datum.setStartTime(startTime);
    datum.setContainerId(new Utf8(containerId.toString()));
    datum.setNodeManagerHost(new Utf8(nodeManagerHost));
    datum.setNodeManagerPort(nodeManagerPort);
    datum.setNodeManagerHttpPort(nodeManagerHttpPort);
    this.forcedJobStateOnShutDown = forcedJobStateOnShutDown;
    this.submitTime = submitTime;
  }

  AMStartedEvent() {
  }

  /**
   * 获取事件的Avro序列化数据对象
   * @return Avro序列化的AM启动事件数据
   */
  public Object getDatum() {
    return datum;
  }

  /**
   * 设置事件的Avro序列化数据对象
   * @param datum Avro序列化的AM启动事件数据
   */
  public void setDatum(Object datum) {
    this.datum = (AMStarted) datum;
  }

  /**
   * 获取应用尝试ID
   * @return 应用尝试ID
   */
  public ApplicationAttemptId getAppAttemptId() {
    return ApplicationAttemptId.fromString(
        datum.getApplicationAttemptId().toString());
  }

  /**
   * 获取AM启动时间
   * @return AM启动时间戳
   */
  public long getStartTime() {
    return datum.getStartTime();
  }

  /**
   * 获取AM运行容器ID
   * @return AM运行所在容器ID
   */
  public ContainerId getContainerId() {
    return ContainerId.fromString(datum.getContainerId().toString());
  }

  /**
   * 获取AM运行所在NodeManager主机地址
   * @return NodeManager主机地址
   */
  public String getNodeManagerHost() {
    return datum.getNodeManagerHost().toString();
  }

  /**
   * 获取AM运行所在NodeManager服务端口
   * @return NodeManager服务端口
   */
  public int getNodeManagerPort() {
    return datum.getNodeManagerPort();
  }
  
  /**
   * 获取AM运行所在NodeManager HTTP服务端口
   * @return NodeManager HTTP服务端口
   */
  public int getNodeManagerHttpPort() {
    return datum.getNodeManagerHttpPort();
  }

  /**
   * 获取AM关闭时强制设置的作业状态
   * @return 强制作业状态字符串
   */
  public String getForcedJobStateOnShutDown() {
    return this.forcedJobStateOnShutDown;
  }

  /**
   * 获取应用(作业)提交时间
   * @return 应用提交时间戳
   */
  public long getSubmitTime() {
    return this.submitTime;
  }

  /**
   * 获取事件类型，返回AM_STARTED事件类型
   * @return 事件类型枚举值
   */
  @Override
  public EventType getEventType() {
    return EventType.AM_STARTED;
  }

  /**
   * 将当前事件转换为YARN时间线服务事件格式
   * @return 转换后的时间线事件对象
   */
  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("APPLICATION_ATTEMPT_ID",
        getAppAttemptId() == null ? "" : getAppAttemptId().toString());
    tEvent.addInfo("CONTAINER_ID", getContainerId() == null ?
        "" : getContainerId().toString());
    tEvent.addInfo("NODE_MANAGER_HOST", getNodeManagerHost());
    tEvent.addInfo("NODE_MANAGER_PORT", getNodeManagerPort());
    tEvent.addInfo("NODE_MANAGER_HTTP_PORT", getNodeManagerHttpPort());
    tEvent.addInfo("START_TIME", getStartTime());
    return tEvent;
  }

  /**
   * 获取事件对应的时间线指标集合，本事件无指标
   * @return 总是返回null
   */
  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }
}