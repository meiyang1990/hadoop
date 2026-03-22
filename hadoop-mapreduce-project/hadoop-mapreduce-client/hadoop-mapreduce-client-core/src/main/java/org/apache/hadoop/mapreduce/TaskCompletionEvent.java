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

package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * 文件：org.apache.hadoop.mapreduce.TaskCompletionEvent
 * 模块：hadoop-mapreduce-client-core
 * 核心职责：用于在JobTracker（历史MR架构）中跟踪任务完成事件，封装任务执行结束后的状态信息
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TaskCompletionEvent implements Writable{
  /**
   * 任务完成状态枚举，定义所有可能的任务执行结果状态
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  static public enum Status {
    /**
     * 本次尝试失败，但仍有剩余尝试次数，任务会继续重试
     */
    FAILED,
    /**
     * 任务尝试被手动杀死
     */
    KILLED,
    /**
     * 任务尝试执行成功
     */
    SUCCEEDED,
    /**
     * 标记之前成功的事件已失效，例如：map任务成功后，shuffle阶段因拉取失败被判定失败，旧的成功事件标记为失效
     */
    OBSOLETE,
    /**
     * 整个任务尝试失败，已达到最大重试次数，reducer收到此事件后会停止从该map拉取数据
     */
    TIPFAILED
  }
    
  private int eventId; 
  private String taskTrackerHttp;
  private int taskRunTime; // using int since runtime is the time difference
  private TaskAttemptID taskId;
  Status status; 
  boolean isMap = false;
  private int idWithinJob;
  public static final TaskCompletionEvent[] EMPTY_ARRAY = 
    new TaskCompletionEvent[0];
  /**
   * 供Writable反序列化使用的默认构造函数
   */
  public TaskCompletionEvent(){
    taskId = new TaskAttemptID();
  }

  /**
   * 构造任务完成事件对象
   * @param eventId 事件ID，每个作业内全局递增唯一，从0开始
   * @param taskId 任务尝试ID
   * @param idWithinJob 任务在作业内的编号
   * @param isMap 是否为Map任务
   * @param status 任务完成状态
   * @param taskTrackerHttp 运行该任务的TaskTracker的Http地址（host:port）
   */
  public TaskCompletionEvent(int eventId, 
                             TaskAttemptID taskId,
                             int idWithinJob,
                             boolean isMap,
                             Status status, 
                             String taskTrackerHttp){
      
    this.taskId = taskId;
    this.idWithinJob = idWithinJob;
    this.isMap = isMap;
    this.eventId = eventId; 
    this.status =status; 
    this.taskTrackerHttp = taskTrackerHttp;
  }
  /**
   * 获取事件ID
   * @return 事件ID
   */
  public int getEventId() {
    return eventId;
  }
  
  /**
   * 获取任务尝试ID
   * @return 任务尝试ID
   */
  public TaskAttemptID getTaskAttemptId() {
    return taskId;
  }
  
  /**
   * 获取任务完成状态
   * @return 任务完成状态枚举值
   */
  public Status getStatus() {
    return status;
  }
  /**
   * 获取运行该任务的TaskTracker的Http地址，用于访问任务日志
   * @return TaskTracker的Http地址
   */
  public String getTaskTrackerHttp() {
    return taskTrackerHttp;
  }

  /**
   * 获取任务执行总耗时（毫秒）
   * @return 任务执行耗时（毫秒）
   */
  public int getTaskRunTime() {
    return taskRunTime;
  }

  /**
   * 设置任务完成耗时
   * @param taskCompletionTime 任务执行耗时（毫秒）
   */
  protected void setTaskRunTime(int taskCompletionTime) {
    this.taskRunTime = taskCompletionTime;
  }

  /**
   * 设置事件ID
   * @param eventId 事件ID
   */
  protected void setEventId(int eventId) {
    this.eventId = eventId;
  }

  /**
   * 设置任务尝试ID
   * @param taskId 任务尝试ID
   */
  protected void setTaskAttemptId(TaskAttemptID taskId) {
    this.taskId = taskId;
  }
  
  /**
   * 设置任务状态
   * @param status 任务状态枚举
   */
  protected void setTaskStatus(Status status) {
    this.status = status;
  }
  
  /**
   * 设置TaskTracker的Http地址
   * @param taskTrackerHttp TaskTracker的Http地址
   */
  protected void setTaskTrackerHttp(String taskTrackerHttp) {
    this.taskTrackerHttp = taskTrackerHttp;
  }
    
  @Override
  public String toString(){
    StringBuilder buf = new StringBuilder();
    buf.append("Task Id : "); 
    buf.append(taskId); 
    buf.append(", Status : ");  
    buf.append(status.name());
    return buf.toString();
  }
    
  @Override
  public boolean equals(Object o) {
    if(o == null)
      return false;
    // 类型不匹配直接返回不相等
    if(o.getClass().equals(this.getClass())) {
      TaskCompletionEvent event = (TaskCompletionEvent) o;
      // 比对所有字段判断是否相等
      return this.isMap == event.isMapTask() 
             && this.eventId == event.getEventId()
             && this.idWithinJob == event.idWithinJob() 
             && this.status.equals(event.getStatus())
             && this.taskId.equals(event.getTaskAttemptId()) 
             && this.taskRunTime == event.getTaskRunTime()
             && this.taskTrackerHttp.equals(event.getTaskTrackerHttp());
    }
    return false;
  }

  @Override
  public int hashCode() {
    // 复用toString计算哈希值
    return toString().hashCode(); 
  }

  /**
   * 判断当前任务是否为Map任务
   * @return true表示Map任务，false表示Reduce任务
   */
  public boolean isMapTask() {
    return isMap;
  }
    
  /**
   * 获取任务在作业内的编号
   * @return 任务在作业内的编号
   */
  public int idWithinJob() {
    return idWithinJob;
  }
  //////////////////////////////////////////////
  // Writable 序列化接口实现
  //////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    // 序列化任务尝试ID
    taskId.write(out); 
    // 可变长度整数序列化作业内编号
    WritableUtils.writeVInt(out, idWithinJob);
    // 序列化是否为Map任务标志
    out.writeBoolean(isMap);
    // 可变长度序列化状态枚举
    WritableUtils.writeEnum(out, status); 
    // 序列化TaskTracker Http地址
    WritableUtils.writeString(out, taskTrackerHttp);
    // 可变长度序列化任务执行耗时
    WritableUtils.writeVInt(out, taskRunTime);
    // 可变长度序列化事件ID
    WritableUtils.writeVInt(out, eventId);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // 反序列化任务尝试ID
    taskId.readFields(in); 
    // 反序列化作业内编号
    idWithinJob = WritableUtils.readVInt(in);
    // 反序列化是否为Map任务标志
    isMap = in.readBoolean();
    // 反序列化状态枚举
    status = WritableUtils.readEnum(in, Status.class);
    // 反序列化TaskTracker Http地址
    taskTrackerHttp = WritableUtils.readString(in);
    // 反序列化任务执行耗时
    taskRunTime = WritableUtils.readVInt(in);
    // 反序列化事件ID
    eventId = WritableUtils.readVInt(in);
  }
}