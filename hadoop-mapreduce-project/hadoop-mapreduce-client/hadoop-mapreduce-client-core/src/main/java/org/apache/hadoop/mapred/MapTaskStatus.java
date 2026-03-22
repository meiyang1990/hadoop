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

/**
 * Map任务状态实现类，存储Map任务执行过程中的状态信息，继承自TaskStatus
 * 负责维护Map任务特有的状态字段，如Map阶段完成时间等
 */
class MapTaskStatus extends TaskStatus {

  // Map任务完成时间戳
  private long mapFinishTime = 0;
  
  public MapTaskStatus() {}

  /**
   * 构造MapTaskStatus实例，初始化所有任务状态信息
   * @param taskid 任务尝试ID
   * @param progress 任务执行进度（0-1）
   * @param numSlots 任务占用的槽位数
   * @param runState 任务运行状态
   * @param diagnosticInfo 诊断信息
   * @param stateString 状态字符串
   * @param taskTracker 任务所在的TaskTracker节点
   * @param phase 任务执行阶段
   * @param counters 任务计数器
   */
  public MapTaskStatus(TaskAttemptID taskid, float progress, int numSlots,
          State runState, String diagnosticInfo, String stateString,
          String taskTracker, Phase phase, Counters counters) {
    super(taskid, progress, numSlots, runState, diagnosticInfo, stateString,
          taskTracker, phase, counters);
  }

  @Override
  /**
   * 判断是否为Map任务，始终返回true
   * @return 固定返回true，表示当前是Map任务状态
   */
  public boolean getIsMap() {
    return true;
  }

  /**
   * 设置任务完成时间，同时如果Map完成时间未设置则更新它
   * @param finishTime 任务完成时间戳
   */
  @Override
  void setFinishTime(long finishTime) {
    super.setFinishTime(finishTime);
    // 如果Map完成时间未设置，将任务完成时间作为Map完成时间
    if (getMapFinishTime() == 0) {
      setMapFinishTime(finishTime);
    }
  }
  
  @Override
  /**
   * 获取Shuffle阶段完成时间，Map任务不支持Shuffle阶段，抛出异常
   * @return 永远不会返回，直接抛出异常
   * @throws UnsupportedOperationException 总是抛出该异常
   */
  public long getShuffleFinishTime() {
    throw new UnsupportedOperationException("getShuffleFinishTime() not supported for MapTask");
  }

  @Override
  /**
   * 设置Shuffle阶段完成时间，Map任务不支持Shuffle阶段，抛出异常
   * @throws UnsupportedOperationException 总是抛出该异常
   */
  void setShuffleFinishTime(long shuffleFinishTime) {
    throw new UnsupportedOperationException("setShuffleFinishTime() not supported for MapTask");
  }

  @Override
  /**
   * 获取Map阶段完成时间戳
   * @return Map阶段完成时间戳
   */
  public long getMapFinishTime() {
    return mapFinishTime;
  }
  
  @Override
  /**
   * 设置Map阶段完成时间戳
   * @param mapFinishTime Map阶段完成时间戳
   */
  void setMapFinishTime(long mapFinishTime) {
    this.mapFinishTime = mapFinishTime;
  }
  
  @Override
  /**
   * 从传入的状态更新当前状态，同步更新Map完成时间
   * @param status 新的任务状态
   */
  synchronized void statusUpdate(TaskStatus status) {
    super.statusUpdate(status);
    // 如果新状态中Map完成时间已设置，更新到当前状态
    if (status.getMapFinishTime() != 0) {
      this.mapFinishTime = status.getMapFinishTime();
    }
  }
  
  @Override
  /**
   * 从输入流反序列化Map任务状态
   * @param in 输入流
   * @throws IOException 反序列化时IO异常
   */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    mapFinishTime = in.readLong();
  }
  
  @Override
  /**
   * 将Map任务状态序列化到输出流
   * @param out 输出流
   * @throws IOException 序列化时IO异常
   */
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(mapFinishTime);
  }

  @Override
  /**
   * 添加获取失败的Map任务记录，Map任务不支持该操作，抛出异常
   * @throws UnsupportedOperationException 总是抛出该异常
   */
  public void addFetchFailedMap(TaskAttemptID mapTaskId) {
    throw new UnsupportedOperationException
                ("addFetchFailedMap() not supported for MapTask");
  }

}