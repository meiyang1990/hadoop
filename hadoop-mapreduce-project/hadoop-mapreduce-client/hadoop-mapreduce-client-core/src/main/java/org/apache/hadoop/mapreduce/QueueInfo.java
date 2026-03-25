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
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringInterner;

/**
 * MapReduce作业队列信息实体类，保存YARN/Hadoop MapReduce框架维护的作业队列基本信息，
 * 包括队列名称、调度信息、队列状态、队列中作业状态以及子队列信息等，支持序列化传输。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QueueInfo implements Writable {

  private String queueName = "";
  
  //The scheduling Information object is read back as String.
  //Once the scheduling information is set there is no way to recover it.
  private String schedulingInfo; 
  
  private QueueState queueState;
  
  // Jobs submitted to the queue
  private JobStatus[] stats;
  
  private List<QueueInfo> children;

  private Properties props;

  /**
   * 无参构造函数，默认初始化队列状态为运行中，创建空的子队列列表和属性对象
   */
  public QueueInfo() {
    // make it running by default.
    this.queueState = QueueState.RUNNING;
    children = new ArrayList<QueueInfo>();
    props = new Properties();
  }
  
  /**
   * 带参数构造函数，通过队列名称和调度信息创建队列信息对象
   * @param queueName 作业队列名称
   * @param schedulingInfo 与队列关联的调度信息
   */
  public QueueInfo(String queueName, String schedulingInfo) {
    this();
    this.queueName = queueName;
    this.schedulingInfo = schedulingInfo;
  }
  
  /**
   * 全参数构造函数，创建包含完整信息的队列信息对象
   * @param queueName 作业队列名称
   * @param schedulingInfo 与队列关联的调度信息
   * @param state 队列当前状态
   * @param stats 队列中已提交作业的状态数组
   */
  public QueueInfo(String queueName, String schedulingInfo, QueueState state,
                   JobStatus[] stats) {
    this(queueName, schedulingInfo);
    this.queueState = state;
    this.stats = stats;
  }

  /**
   * 设置队列名称
   * @param queueName 作业队列名称
   */
  protected void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  /**
   * 获取队列名称
   * @return 队列名称
   */
  public String getQueueName() {
    return queueName;
  }

  /**
   * 设置队列对应的调度信息
   * @param schedulingInfo 调度信息字符串
   */
  protected void setSchedulingInfo(String schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }

  /**
   * 获取队列对应的调度信息，如果未设置则返回默认值"N/A"
   * @return 队列关联的调度信息字符串
   */
  public String getSchedulingInfo() {
    if(schedulingInfo != null) {
      return schedulingInfo;
    }else {
      return "N/A";
    }
  }
  
  /**
   * 设置队列当前状态
   * @param state 队列状态对象
   */
  protected void setState(QueueState state) {
    queueState = state;
  }
  
  /**
   * 获取队列当前状态
   * @return 队列状态对象
   */
  public QueueState getState() {
    return queueState;
  }
  
  protected void setJobStatuses(JobStatus[] stats) {
    this.stats = stats;
  }

  /** 
   * 获取当前队列的所有直接子队列信息
   * @return 子队列信息列表
   */
  public List<QueueInfo> getQueueChildren() {
    return children;
  }

  protected void setQueueChildren(List<QueueInfo> children) {
    this.children =  children; 
  }

  /**
   * 获取队列自定义属性集合
   * @return 队列属性Properties对象
   */
  public Properties getProperties() {
    return props;
  }

  protected void setProperties(Properties props) {
    this.props = props;
  }

  /**
   * 获取当前队列中所有已提交作业的状态数组
   * @return 作业状态数组
   */
  public JobStatus[] getJobStatuses() {
    return stats;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // 从输入流读取并 intern 队列名称，减少内存占用
    queueName = StringInterner.weakIntern(Text.readString(in));
    // 从输入流读取队列状态枚举
    queueState = WritableUtils.readEnum(in, QueueState.class);
    // 从输入流读取并 intern 调度信息
    schedulingInfo = StringInterner.weakIntern(Text.readString(in));
    // 读取作业状态数组长度
    int length = in.readInt();
    stats = new JobStatus[length];
    // 逐个读取作业状态信息
    for (int i = 0; i < length; i++) {
      stats[i] = new JobStatus();
      stats[i].readFields(in);
    }
    // 读取子队列数量
    int count = in.readInt();
    children.clear();
    // 逐个读取子队列信息
    for (int i = 0; i < count; i++) {
      QueueInfo childQueueInfo = new QueueInfo();
      childQueueInfo.readFields(in);
      children.add(childQueueInfo);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // 将队列名称写入输出流
    Text.writeString(out, queueName);
    // 将队列状态枚举写入输出流
    WritableUtils.writeEnum(out, queueState);
    
    // 处理调度信息，空值写入默认值"N/A"
    if(schedulingInfo!= null) {
      Text.writeString(out, schedulingInfo);
    }else {
      Text.writeString(out, "N/A");
    }
    // 写入作业状态数组长度
    out.writeInt(stats.length);
    // 逐个写入作业状态
    for (JobStatus stat : stats) {
      stat.write(out);
    }
    // 写入子队列数量
    out.writeInt(children.size());
    // 逐个写入子队列信息
    for(QueueInfo childQueueInfo : children) {
      childQueueInfo.write(out);
    }
  }
}