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

package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件说明：MapReduce v1 API 任务完成事件类，用于在JobTracker端追踪任务完成状态变更
 * 继承自新版本org.apache.hadoop.mapreduce.TaskCompletionEvent，兼容旧版API
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TaskCompletionEvent 
    extends org.apache.hadoop.mapreduce.TaskCompletionEvent {
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  /**
   * 任务完成状态枚举，定义任务执行结束后的所有可能状态
   */
  static public enum Status {
    /**
     * 任务尝试失败，但仍有剩余重试次数，可继续重试
     */
    FAILED,
    /**
     * 任务被系统主动杀死
     */
    KILLED,
    /**
     * 任务执行成功完成
     */
    SUCCEEDED,
    /**
     * 标记之前成功的事件已失效，例如：Map任务执行成功后，因shuffle阶段拉取失败被判定为失败，原成功事件标记为OBSOLETE
     */
    OBSOLETE,
    /**
     * 任务尝试已达到最大重试次数，整体任务失败，Reduce端收到该事件后会停止从该Map任务拉取数据
     */
    TIPFAILED
  }
  
  // 空任务完成事件数组，用于返回空结果场景
  public static final TaskCompletionEvent[] EMPTY_ARRAY = 
	    new TaskCompletionEvent[0];
  /**
   * Writable序列化默认构造函数
   */
  public TaskCompletionEvent() {
    super();
  }

  /**
   * 构造任务完成事件对象
   * @param eventId 事件ID，每个作业内递增唯一，从0开始
   * @param taskId 任务尝试ID
   * @param idWithinJob 任务在作业中的编号
   * @param isMap 是否为Map任务
   * @param status 任务完成状态
   * @param taskTrackerHttp TaskTracker的HTTP地址（host:port格式）
   */
  public TaskCompletionEvent(int eventId, 
                             TaskAttemptID taskId,
                             int idWithinJob,
                             boolean isMap,
                             Status status, 
                             String taskTrackerHttp){
    super(eventId, taskId, idWithinJob, isMap, org.apache.hadoop.mapreduce.
          TaskCompletionEvent.Status.valueOf(status.name()), taskTrackerHttp);
  }

  /**
   * 将新版TaskCompletionEvent对象降级转换为旧版API对象，兼容旧版接口
   * @param event 新版API的任务完成事件对象
   * @return 转换后的旧版API任务完成事件对象
   */
  @Private
  public static TaskCompletionEvent downgrade(
    org.apache.hadoop.mapreduce.TaskCompletionEvent event) {
    return new TaskCompletionEvent(event.getEventId(),
      TaskAttemptID.downgrade(event.getTaskAttemptId()),event.idWithinJob(),
      event.isMapTask(), Status.valueOf(event.getStatus().name()),
      event.getTaskTrackerHttp());
  }
  /**
   * 获取任务ID
   * @return 任务ID字符串
   * @deprecated 已废弃，请使用{@link #getTaskAttemptId()}方法
   */
  @Deprecated
  public String getTaskId() {
    return getTaskAttemptId().toString();
  }
  
  /**
   * 获取任务尝试ID
   * @return 旧版API的任务尝试ID对象
   */
  public TaskAttemptID getTaskAttemptId() {
    return TaskAttemptID.downgrade(super.getTaskAttemptId());
  }
  
  /**
   * 获取任务完成状态
   * @return 任务完成状态枚举值
   */
  public Status getTaskStatus() {
    return Status.valueOf(super.getStatus().name());
  }
  
  /**
   * 设置任务ID
   * @param taskId 任务ID字符串
   * @deprecated 已废弃，请使用{@link #setTaskAttemptId(TaskAttemptID)}方法
   */
  @Deprecated
  public void setTaskId(String taskId) {
    this.setTaskAttemptId(TaskAttemptID.forName(taskId));
  }

  /**
   * 设置任务尝试ID
   * @param taskId 任务尝试ID对象
   * @deprecated 已废弃，请使用{@link #setTaskAttemptId(TaskAttemptID)}方法
   */
  @Deprecated
  public void setTaskID(TaskAttemptID taskId) {
    this.setTaskAttemptId(taskId);
  }

  /**
   * 设置任务尝试ID
   * @param taskId 任务尝试ID对象
   */
  protected void setTaskAttemptId(TaskAttemptID taskId) {
    super.setTaskAttemptId(taskId);
  }
  
  /**
   * 设置任务完成状态
   * @param status 任务完成状态枚举值
   */
  @Private
  public void setTaskStatus(Status status) {
    super.setTaskStatus(org.apache.hadoop.mapreduce.
      TaskCompletionEvent.Status.valueOf(status.name()));
  }
  
  /**
   * 设置任务运行总时长
   * @param taskCompletionTime 任务运行耗时，单位毫秒
   */
  @Private
  public void setTaskRunTime(int taskCompletionTime) {
    super.setTaskRunTime(taskCompletionTime);
  }

  /**
   * 设置事件ID，需从0开始递增分配
   * @param eventId 事件ID
   */
  @Private
  public void setEventId(int eventId) {
    super.setEventId(eventId);
  }

  /**
   * 设置TaskTracker的HTTP访问地址
   * @param taskTrackerHttp TaskTracker的HTTP地址（host:port格式）
   */
  @Private
  public void setTaskTrackerHttp(String taskTrackerHttp) {
    super.setTaskTrackerHttp(taskTrackerHttp);
  }
}