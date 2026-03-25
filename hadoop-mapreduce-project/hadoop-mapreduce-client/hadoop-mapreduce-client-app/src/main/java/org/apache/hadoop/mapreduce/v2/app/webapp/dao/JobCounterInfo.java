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
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.util.MRApps;

/**
 * 作业计数器信息数据访问对象，为MapReduce应用Web界面提供作业计数器聚合数据
 * 负责聚合整个作业、Map阶段、Reduce阶段的计数器信息，并转换为Web可序列化格式
 */
@XmlRootElement(name = "jobCounters")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobCounterInfo {

  @XmlTransient
  protected Counters total = null;
  @XmlTransient
  protected Counters map = null;
  @XmlTransient
  protected Counters reduce = null;

  protected String id;
  protected ArrayList<CounterGroupInfo> counterGroup;

  /**
   * JAXB反序列化需要的无参构造函数
   */
  public JobCounterInfo() {
  }

  /**
   * 构造作业计数器信息，聚合作业所有任务的计数器数据
   * @param ctx 应用上下文，提供应用运行环境信息
   * @param job 目标作业对象，从中获取任务和计数器数据
   */
  public JobCounterInfo(AppContext ctx, Job job) {
    // 分别聚合总、Map、Reduce阶段计数器
    getCounters(ctx, job);
    counterGroup = new ArrayList<CounterGroupInfo>();
    // 转换作业ID为字符串格式
    this.id = MRApps.toString(job.getID());

    if (total != null) {
      // 遍历所有计数器分组，构建分组信息对象
      for (CounterGroup g : total) {
        if (g != null) {
          // 获取Map阶段对应分组计数器
          CounterGroup mg = map == null ? null : map.getGroup(g.getName());
          // 获取Reduce阶段对应分组计数器
          CounterGroup rg = reduce == null ? null : reduce
            .getGroup(g.getName());

          // 构建计数器分组信息并添加到列表
          CounterGroupInfo cginfo = new CounterGroupInfo(g.getName(), g,
            mg, rg);
          counterGroup.add(cginfo);
        }
      }
    }
  }

  /**
   * 从作业的所有任务中聚合出总、Map、Reduce三个维度的计数器
   * @param ctx 应用上下文
   * @param job 目标作业对象
   */
  private void getCounters(AppContext ctx, Job job) {
    if (job == null) {
      return;
    }
    // 优先获取作业已经聚合好的全局计数器
    total = job.getAllCounters();
    boolean needTotalCounters = false;
    // 如果作业没有预聚合的计数器，则需要自己从任务聚合
    if (total == null) {
      total = new Counters();
      needTotalCounters = true;
    }
    // 初始化Map、Reduce阶段计数器容器
    map = new Counters();
    reduce = new Counters();
    // 获取作业所有任务列表
    Map<TaskId, Task> tasks = job.getTasks();
    // 遍历所有任务，按任务类型累加计数器
    for (Task t : tasks.values()) {
      Counters counters = t.getCounters();
      if (counters == null) {
        continue;
      }
      // 根据任务类型累加计数器到对应维度
      switch (t.getType()) {
      case MAP:
        map.incrAllCounters(counters);
        break;
      case REDUCE:
        reduce.incrAllCounters(counters);
        break;
      }
      // 如果需要自行聚合总计数器，则累加当前任务计数器
      if (needTotalCounters) {
        total.incrAllCounters(counters);
      }
    }
  }

}