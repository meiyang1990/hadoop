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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.util.StringInterner;

/**
 * 任务状态报告类，保存MapReduce单个任务的运行状态、进度、诊断信息等运行时数据
 * 用于在集群各组件之间传递任务状态信息，供监控和调度使用
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TaskReport implements Writable {
  private TaskID taskid;
  private float progress;
  private String state;
  private String[] diagnostics;
  private long startTime; 
  private long finishTime; 
  private Counters counters;
  private TIPStatus currentStatus;
  
  // 当前正在运行的任务尝试列表
  private Collection<TaskAttemptID> runningAttempts = 
    new ArrayList<TaskAttemptID>();
  // 成功完成的任务尝试ID
  private TaskAttemptID successfulAttempt = new TaskAttemptID();

  /**
   * 构造空的任务报告对象，用于反序列化
   */
  public TaskReport() {
    taskid = new TaskID();
  }
  
  /**
   * 构造完整的任务报告对象，封装任务的所有状态信息
   * @param taskid 任务ID
   * @param progress 任务完成进度 0~1
   * @param state 任务状态描述
   * @param diagnostics 诊断错误信息数组
   * @param currentStatus 任务当前整体状态
   * @param startTime 任务开始时间戳
   * @param finishTime 任务完成时间戳
   * @param counters 任务计数器集合
   */
  public TaskReport(TaskID taskid, float progress, String state,
             String[] diagnostics, TIPStatus currentStatus, 
             long startTime, long finishTime,
             Counters counters) {
    this.taskid = taskid;
    this.progress = progress;
    this.state = state;
    this.diagnostics = diagnostics;
    this.currentStatus = currentStatus;
    this.startTime = startTime; 
    this.finishTime = finishTime;
    this.counters = counters;
  }

  /**
   * 获取任务ID的字符串表示
   * @return 任务ID字符串
   */
  public String getTaskId() {
    return taskid.toString();
  }

  /**
   * 获取任务ID对象
   * @return 任务ID对象
   */
  public TaskID getTaskID() {
    return taskid;
  }

  /**
   * 获取任务完成进度
   * @return 进度值，范围0~1
   */
  public float getProgress() { return progress; }
  
  /**
   * 获取任务当前状态描述
   * @return 状态描述字符串
   */
  public String getState() { return state; }
  
  /**
   * 获取诊断错误信息列表
   * @return 错误信息数组
   */
  public String[] getDiagnostics() { return diagnostics; }
  
  /**
   * 获取任务计数器集合
   * @return 任务计数器集合
   */
  public Counters getTaskCounters() { return counters; }
  
  /**
   * 获取任务当前整体状态
   * @return 任务状态枚举值
   */
  public TIPStatus getCurrentStatus() {
    return currentStatus;
  }
  
  /**
   * 获取任务完成时间戳
   * @return 完成时间戳，未设置则返回0
   */
  public long getFinishTime() {
    return finishTime;
  }

  /**
   * 设置任务完成时间戳
   * @param finishTime 任务完成时间戳
   */
  protected void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  /**
   * 获取任务开始时间戳
   * @return 开始时间戳，未设置则返回0
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * 设置任务开始时间戳
   * @param startTime 任务开始时间戳
   */ 
  protected void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * 设置任务成功完成的尝试ID
   * @param t 成功的任务尝试ID
   */ 
  protected void setSuccessfulAttemptId(TaskAttemptID t) {
    successfulAttempt = t;
  }
  
  /**
   * 获取成功完成该任务的尝试ID
   * @return 成功的任务尝试ID
   */
  public TaskAttemptID getSuccessfulTaskAttemptId() {
    return successfulAttempt;
  }
  
  /**
   * 设置当前正在运行的任务尝试列表
   * @param runningAttempts 正在运行的任务尝试ID集合
   */ 
  protected void setRunningTaskAttemptIds(
      Collection<TaskAttemptID> runningAttempts) {
    this.runningAttempts = runningAttempts;
  }
  
  /**
   * 获取当前正在运行的任务尝试ID集合
   * @return 正在运行的任务尝试ID集合
   */
  public Collection<TaskAttemptID> getRunningTaskAttemptIds() {
    return runningAttempts;
  }


  @Override
  public boolean equals(Object o) {
    if(o == null)
      return false;
    if(o.getClass().equals(this.getClass())) {
      TaskReport report = (TaskReport) o;
      // 逐个对比所有字段判断是否相等
      return counters.equals(report.getTaskCounters())
             && Arrays.toString(this.diagnostics)
                      .equals(Arrays.toString(report.getDiagnostics()))
             && this.finishTime == report.getFinishTime()
             && this.progress == report.getProgress()
             && this.startTime == report.getStartTime()
             && this.state.equals(report.getState())
             && this.taskid.equals(report.getTaskID());
    }
    return false; 
  }

  @Override
  public int hashCode() {
    // 基于所有字段计算哈希值
    return (counters.toString() + Arrays.toString(this.diagnostics) 
            + this.finishTime + this.progress + this.startTime + this.state 
            + this.taskid.toString()).hashCode();
  }
  //////////////////////////////////////////////
  // Writable 序列化接口实现
  //////////////////////////////////////////////

  @Override
  public void write(DataOutput out) throws IOException {
    // 写入任务ID
    taskid.write(out);
    // 写入进度
    out.writeFloat(progress);
    // 写入状态字符串
    Text.writeString(out, state);
    // 写入起止时间
    out.writeLong(startTime);
    out.writeLong(finishTime);
    // 写入诊断信息数组
    WritableUtils.writeStringArray(out, diagnostics);
    // 写入计数器
    counters.write(out);
    // 写入当前状态枚举
    WritableUtils.writeEnum(out, currentStatus);
    // 根据状态写入不同的尝试信息
    if (currentStatus == TIPStatus.RUNNING) {
      // 运行中：写入所有正在运行的尝试
      WritableUtils.writeVInt(out, runningAttempts.size());
      TaskAttemptID t[] = new TaskAttemptID[0];
      t = runningAttempts.toArray(t);
      for (int i = 0; i < t.length; i++) {
        t[i].write(out);
      }
    } else if (currentStatus == TIPStatus.COMPLETE) {
      // 已完成：只写入成功的尝试
      successfulAttempt.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // 读取任务ID
    this.taskid.readFields(in);
    // 读取进度
    this.progress = in.readFloat();
    // 读取状态字符串，使用弱引用驻留字符串节省内存
    this.state = StringInterner.weakIntern(Text.readString(in));
    // 读取起止时间
    this.startTime = in.readLong(); 
    this.finishTime = in.readLong();
    
    // 读取诊断信息数组
    diagnostics = WritableUtils.readStringArray(in);
    // 读取计数器
    counters = new Counters();
    counters.readFields(in);
    // 读取当前状态枚举
    currentStatus = WritableUtils.readEnum(in, TIPStatus.class);
    // 根据状态读取不同的尝试信息
    if (currentStatus == TIPStatus.RUNNING) {
      // 运行中：读取所有正在运行的尝试
      int num = WritableUtils.readVInt(in);    
      for (int i = 0; i < num; i++) {
        TaskAttemptID t = new TaskAttemptID();
        t.readFields(in);
        runningAttempts.add(t);
      }
    } else if (currentStatus == TIPStatus.COMPLETE) {
      // 已完成：读取成功的尝试
      successfulAttempt.readFields(in);
    }
  }
}