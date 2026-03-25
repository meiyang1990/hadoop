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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.QueueState;

/**
 * 文件说明：MapReduce v1 API 作业队列信息实体类，保存Hadoop MapReduce框架维护的作业队列相关信息
 * 继承新版QueueInfo，兼容旧版mapred API的使用需求
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JobQueueInfo extends QueueInfo {

  /**
   * 构造函数：创建空的作业队列信息对象
   */
  public JobQueueInfo() {
    super();  
  }

  /**
   * 构造函数：根据队列名称和调度信息创建作业队列信息对象
   * 
   * @param queueName 作业队列名称
   * @param schedulingInfo 关联作业队列的调度信息
   */
  public JobQueueInfo(String queueName, String schedulingInfo) {
    super(queueName, schedulingInfo);
  }
  
  /**
   * 构造函数：从新版QueueInfo转换为旧版JobQueueInfo
   * 完成队列状态、子队列、属性和作业状态信息的拷贝
   * 
   * @param queue 新版QueueInfo对象
   */
  JobQueueInfo(QueueInfo queue) {
    this(queue.getQueueName(), queue.getSchedulingInfo());
    setQueueState(queue.getState().getStateName());
    setQueueChildren(queue.getQueueChildren());
    setProperties(queue.getProperties());
    setJobStatuses(queue.getJobStatuses());
  }
  
  /**
   * 设置作业队列名称
   * 
   * @param queueName 作业队列名称
   */
  @InterfaceAudience.Private
  public void setQueueName(String queueName) {
    super.setQueueName(queueName);
  }

  /**
   * 设置作业队列关联的调度信息
   * 
   * @param schedulingInfo 调度信息字符串
   */
  @InterfaceAudience.Private
  public void setSchedulingInfo(String schedulingInfo) {
    super.setSchedulingInfo(schedulingInfo);
  }

  /**
   * 设置队列当前状态
   * @param state 队列状态名称
   */
  @InterfaceAudience.Private
  public void setQueueState(String state) {
    super.setState(QueueState.getState(state));
  }
  
  /**
   * 获取队列状态，已废弃，请使用getState()替代
   */
  @Deprecated
  public String getQueueState() {
    return super.getState().toString();
  }
  
  /**
   * 设置当前队列的子队列列表
   * @param children 子队列列表（旧版JobQueueInfo类型）
   */
  @InterfaceAudience.Private
  public void setChildren(List<JobQueueInfo> children) {
    List<QueueInfo> list = new ArrayList<QueueInfo>();
    for (JobQueueInfo q : children) {
      list.add(q);
    }
    super.setQueueChildren(list);
  }

  /**
   * 获取当前队列的子队列列表
   * @return 子队列列表（旧版JobQueueInfo类型）
   */
  public List<JobQueueInfo> getChildren() {
    List<JobQueueInfo> list = new ArrayList<JobQueueInfo>();
    for (QueueInfo q : super.getQueueChildren()) {
      list.add((JobQueueInfo)q);
    }
    return list;
  }

  /**
   * 设置队列自定义属性
   * @param props 自定义属性对象
   */
  @InterfaceAudience.Private
  public void setProperties(Properties props) {
    super.setProperties(props);
  }

  /**
   * 添加子队列到当前队列，更新子队列全限定名反映层级关系
   * 仅用于测试场景
   * 
   * @param child 要添加的子队列
   */
  void addChild(JobQueueInfo child) {
    List<JobQueueInfo> children = getChildren();
    children.add(child);
    setChildren(children);
  }

  /**
   * 从当前队列移除子队列，将子队列名称从全限定名重置为简单名称
   * 仅用于测试场景
   * 
   * @param child 要移除的子队列
   */
  void removeChild(JobQueueInfo child) {
    List<JobQueueInfo> children = getChildren();
    children.remove(child);
    setChildren(children);
  }

  /**
   * 设置队列中当前作业状态数组
   * @param stats 作业状态数组
   */
  @InterfaceAudience.Private
  public void setJobStatuses(org.apache.hadoop.mapreduce.JobStatus[] stats) {
    super.setJobStatuses(stats);
  }

}