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

import org.apache.hadoop.io.Writable;

/**
 * 封装来自ApplicationMaster的反馈信息，包含任务查找结果和资源抢占请求
 * 该类用于MR AppMaster和TaskTracker之间传递任务调度相关反馈，实现Writable支持序列化
 */
public class AMFeedback implements Writable {

  boolean taskFound;
  boolean preemption;

  /**
   * 设置任务是否找到标志
   * @param t 是否找到任务
   */
  public void setTaskFound(boolean t){
    taskFound=t;
  }

  /**
   * 获取任务是否找到标志
   * @return 任务是否找到
   */
  public boolean getTaskFound(){
    return taskFound;
  }

  /**
   * 设置是否需要资源抢占
   * @param preemption 是否需要抢占
   */
  public void setPreemption(boolean preemption) {
    this.preemption=preemption;
  }

  /**
   * 获取是否需要资源抢占
   * @return 是否需要抢占资源
   */
  public boolean getPreemption() {
    return preemption;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // 写入任务找到标志
    out.writeBoolean(taskFound);
    // 写入资源抢占标志
    out.writeBoolean(preemption);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // 读取任务找到标志
    taskFound = in.readBoolean();
    // 读取资源抢占标志
    preemption = in.readBoolean();
  }

}