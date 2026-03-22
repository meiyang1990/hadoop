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

package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;

/**
 * MapReduce任务尝试计数器信息数据访问对象，用于Web UI序列化展示任务尝试的计数器信息
 * 将任务尝试的所有计数器分组整理为可通过JAXB序列化输出XML/JSON格式的数据结构
 */
@XmlRootElement(name = "jobTaskAttemptCounters")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobTaskAttemptCounterInfo {

  @XmlTransient
  protected Counters total = null;

  protected String id;
  protected ArrayList<TaskCounterGroupInfo> taskAttemptCounterGroup;

  /**
   * 默认无参构造函数，供JAXB序列化使用
   */
  public JobTaskAttemptCounterInfo() {
  }

  /**
   * 根据任务尝试实例构建计数器信息对象，整理所有计数器分组
   * @param taskattempt 目标任务尝试实例，从中提取计数器信息
   */
  public JobTaskAttemptCounterInfo(TaskAttempt taskattempt) {

    // 转换任务尝试ID为字符串用于展示
    this.id = MRApps.toString(taskattempt.getID());
    // 获取任务尝试的全部计数器
    total = taskattempt.getCounters();
    // 初始化计数器分组列表
    taskAttemptCounterGroup = new ArrayList<TaskCounterGroupInfo>();
    // 如果存在计数器，遍历所有分组并封装信息
    if (total != null) {
      for (CounterGroup g : total) {
        if (g != null) {
          // 封装单个计数器分组信息
          TaskCounterGroupInfo cginfo = new TaskCounterGroupInfo(g.getName(), g);
          if (cginfo != null) {
            taskAttemptCounterGroup.add(cginfo);
          }
        }
      }
    }
  }
}