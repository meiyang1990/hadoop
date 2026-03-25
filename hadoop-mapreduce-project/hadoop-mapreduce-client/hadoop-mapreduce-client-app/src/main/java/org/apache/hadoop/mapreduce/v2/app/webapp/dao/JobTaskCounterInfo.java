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
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.util.MRApps;

/**
 * 任务计数器信息数据传输对象，用于MapReduce Application WebUI展示单个任务的计数器信息
 * 将任务的所有计数器分组整理，提供XML/JSON序列化支持
 */
@XmlRootElement(name = "jobTaskCounters")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobTaskCounterInfo {

  @XmlTransient
  protected Counters total = null;

  protected String id;
  protected ArrayList<TaskCounterGroupInfo> taskCounterGroup;

  /**
   * 默认无参构造函数，供JAXB序列化使用
   */
  public JobTaskCounterInfo() {
  }

  /**
   * 从任务对象构造任务计数器信息，整理所有计数器分组
   * @param task 目标任务对象，从中提取计数器信息
   */
  public JobTaskCounterInfo(Task task) {
    total = task.getCounters();
    this.id = MRApps.toString(task.getID());
    taskCounterGroup = new ArrayList<TaskCounterGroupInfo>();
    if (total != null) {
      // 遍历所有计数器分组，逐个转换为WebDAO对象
      for (CounterGroup g : total) {
        if (g != null) {
          TaskCounterGroupInfo cginfo = new TaskCounterGroupInfo(g.getName(), g);
          taskCounterGroup.add(cginfo);
        }
      }
    }
  }
}