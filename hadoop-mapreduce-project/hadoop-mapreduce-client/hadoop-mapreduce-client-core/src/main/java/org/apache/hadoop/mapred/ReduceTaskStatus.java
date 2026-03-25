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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reduce任务状态信息类，继承TaskStatus，维护Reduce任务执行各阶段的时间点和获取失败的Map任务列表
 * 用于在Hadoop MapReduce框架中跟踪Reduce任务的执行状态
 */
class ReduceTaskStatus extends TaskStatus {

  // Shuffle阶段完成时间
  private long shuffleFinishTime; 
  // Sort阶段完成时间
  private long sortFinishTime; 
  // 获取失败的Map任务尝试ID列表
  private List<TaskAttemptID> failedFetchTasks = new ArrayList<TaskAttemptID>(1);
  
  public ReduceTaskStatus() {}

  /**
   * 构造Reduce任务状态对象，初始化所有状态信息
   * @param taskid 任务尝试ID
   * @param progress 任务进度（0-1）
   * @param numSlots 任务占用的slot数量
   * @param runState 任务运行状态
   * @param diagnosticInfo 诊断信息
   * @param stateString 状态描述字符串
   * @param taskTracker 运行该任务的TaskTracker地址
   * @param phase 任务执行阶段
   * @param counters 任务计数器
   */
  public ReduceTaskStatus(TaskAttemptID taskid, float progress, int numSlots,
                          State runState, String diagnosticInfo, String stateString, 
                          String taskTracker, Phase phase, Counters counters) {
    super(taskid, progress, numSlots, runState, diagnosticInfo, stateString, 
          taskTracker, phase, counters);
  }

  @Override
  public Object clone() {
    ReduceTaskStatus myClone = (ReduceTaskStatus)super.clone();
    // 深拷贝失败获取任务列表，避免修改原始对象
    myClone.failedFetchTasks = new ArrayList<TaskAttemptID>(failedFetchTasks);
    return myClone;
  }

  @Override
  public boolean getIsMap() {
    // 当前是Reduce任务，固定返回false
    return false;
  }

  @Override
  void setFinishTime(long finishTime) {
    // 如果shuffle完成时间未设置，则使用任务完成时间填充
    if (shuffleFinishTime == 0) {
      this.shuffleFinishTime = finishTime; 
    }
    // 如果sort完成时间未设置，则使用任务完成时间填充
    if (sortFinishTime == 0){
      this.sortFinishTime = finishTime;
    }
    super.setFinishTime(finishTime);
  }

  @Override
  public long getShuffleFinishTime() {
    return shuffleFinishTime;
  }

  @Override
  void setShuffleFinishTime(long shuffleFinishTime) {
    this.shuffleFinishTime = shuffleFinishTime;
  }

  @Override
  public long getSortFinishTime() {
    return sortFinishTime;
  }

  @Override
  void setSortFinishTime(long sortFinishTime) {
    this.sortFinishTime = sortFinishTime;
    // 如果shuffle完成时间未设置，则使用sort完成时间填充
    if (0 == this.shuffleFinishTime){
      this.shuffleFinishTime = sortFinishTime;
    }
  }

  @Override
  public long getMapFinishTime() {
    // Reduce任务不支持获取Map完成时间，抛出不支持操作异常
    throw new UnsupportedOperationException(
        "getMapFinishTime() not supported for ReduceTask");
  }

  @Override
  void setMapFinishTime(long shuffleFinishTime) {
    // Reduce任务不支持设置Map完成时间，抛出不支持操作异常
    throw new UnsupportedOperationException(
        "setMapFinishTime() not supported for ReduceTask");
  }

  @Override
  public List<TaskAttemptID> getFetchFailedMaps() {
    return failedFetchTasks;
  }
  
  @Override
  public void addFetchFailedMap(TaskAttemptID mapTaskId) {
    // 添加一个获取失败的Map任务到列表
    failedFetchTasks.add(mapTaskId);
  }
  
  @Override
  synchronized void statusUpdate(TaskStatus status) {
    super.statusUpdate(status);
    
    // 如果更新状态中包含shuffle完成时间，则更新本地记录
    if (status.getShuffleFinishTime() != 0) {
      this.shuffleFinishTime = status.getShuffleFinishTime();
    }
    
    // 如果更新状态中包含sort完成时间，则更新本地记录
    if (status.getSortFinishTime() != 0) {
      sortFinishTime = status.getSortFinishTime();
    }
    
    // 合并新增的获取失败Map任务列表
    List<TaskAttemptID> newFetchFailedMaps = status.getFetchFailedMaps();
    if (failedFetchTasks == null) {
      failedFetchTasks = newFetchFailedMaps;
    } else if (newFetchFailedMaps != null){
      failedFetchTasks.addAll(newFetchFailedMaps);
    }
  }

  @Override
  synchronized void clearStatus() {
    super.clearStatus();
    // 清空获取失败任务列表
    failedFetchTasks.clear();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    // 反序列化shuffle完成时间
    shuffleFinishTime = in.readLong(); 
    // 反序列化sort完成时间
    sortFinishTime = in.readLong();
    // 反序列化获取失败任务数量
    int noFailedFetchTasks = in.readInt();
    failedFetchTasks = new ArrayList<TaskAttemptID>(noFailedFetchTasks);
    // 逐个反序列化获取失败的任务ID并添加到列表
    for (int i=0; i < noFailedFetchTasks; ++i) {
      TaskAttemptID id = new TaskAttemptID();
      id.readFields(in);
      failedFetchTasks.add(id);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    // 序列化shuffle完成时间
    out.writeLong(shuffleFinishTime);
    // 序列化sort完成时间
    out.writeLong(sortFinishTime);
    // 序列化获取失败任务数量
    out.writeInt(failedFetchTasks.size());
    // 逐个序列化获取失败的任务ID
    for (TaskAttemptID taskId : failedFetchTasks) {
      taskId.write(out);
    }
  }
  
}