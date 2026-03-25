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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.COUNTER_GROUP;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.COUNTER_NAME;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * 单个计数器展示Block，用于在MapReduce Application WebUI中展示指定计数器在各任务/任务尝试下的值
 * 支持按Job级（展示所有任务的该计数器值）和Task级（展示该任务所有尝试的该计数器值）两种场景展示
 */
public class SingleCounterBlock extends HtmlBlock {
  protected TreeMap<String, Long> values = new TreeMap<String, Long>(); 
  protected Job job;
  protected Task task;
  private TaskType counterType;
  
  /**
   * 构造方法，注入应用上下文和视图上下文，初始化成员数据
   * @param appCtx 应用上下文，可获取作业任务信息
   * @param ctx 视图上下文，可获取请求参数
   */
  @Inject SingleCounterBlock(AppContext appCtx, ViewContext ctx) {
    super(ctx);
    this.populateMembers(appCtx);
  }

  /**
   * 渲染单个计数器表格HTML
   * @param html HTML块构建对象
   */
  @Override protected void render(Block html) {
    // 作业不存在时返回错误提示
    if (job == null) {
      html.
        p().__("Sorry, no counters for nonexistent", $(JOB_ID, "job")).__();
      return;
    }
    // 请求参数包含任务ID但任务不存在时返回错误提示
    if (!$(TASK_ID).isEmpty() && task == null) {
      html.
        p().__("Sorry, no counters for nonexistent", $(TASK_ID, "task")).__();
      return;
    }
    
    // 根据当前是作业级还是任务级展示，设置表格表头名称
    String columnType = task == null ? "Task" : "Task Attempt";
    
    // 构建表格框架和表头
    TBODY<TABLE<DIV<Hamlet>>> tbody = html.
      div(_INFO_WRAP).
      table("#singleCounter").
        thead().
          tr().
            th(".ui-state-default", columnType).
            th(".ui-state-default", "Value").__().__().
          tbody();
    // 遍历所有计数器值，生成表格行
    for (Map.Entry<String, Long> entry : values.entrySet()) {
      TR<TBODY<TABLE<DIV<Hamlet>>>> row = tbody.tr();
      String id = entry.getKey();
      String val = entry.getValue().toString();
      if(task != null) {
        // 任务尝试级场景，直接展示值，不需要跳转链接
        row.td(id);
        row.td().br().$title(val).__().__(val).__();
      } else {
        // 任务级场景，添加到任务计数器详情页的跳转链接
        row.td().a(url("singletaskcounter",entry.getKey(),
            $(COUNTER_GROUP), $(COUNTER_NAME)), id).__();
        row.td().br().$title(val).__().a(url("singletaskcounter", entry.getKey(),
            $(COUNTER_GROUP), $(COUNTER_NAME)), val).__();
      }
      row.__();
    }
    // 闭合HTML标签
    tbody.__().__().__();
  }

  /**
   * 根据请求参数从应用上下文中提取需要的计数器数据，填充到成员变量中
   * @param ctx 应用上下文，用于获取作业和任务信息
   */
  private void populateMembers(AppContext ctx) {
    JobId jobID = null;
    TaskId taskID = null;
    String tid = $(TASK_ID);
    // 根据页面标题判断当前需要统计的任务类型（Map/Reduce）
    if ($(TITLE).contains("MAPS")) {
      counterType = TaskType.MAP;
    } else if ($(TITLE).contains("REDUCES")) {
      counterType = TaskType.REDUCE;
    } else {
      counterType = null;
    }
    // 从请求参数解析任务ID，关联对应作业ID
    if (!tid.isEmpty()) {
      taskID = MRApps.toTaskID(tid);
      jobID = taskID.getJobId();
    } else {
      // 从请求参数解析作业ID
      String jid = $(JOB_ID);
      if (!jid.isEmpty()) {
        jobID = MRApps.toJobID(jid);
      }
    }
    // 未解析到有效作业ID，直接返回
    if (jobID == null) {
      return;
    }
    // 从上下文中获取作业对象
    job = ctx.getJob(jobID);
    // 作业不存在，直接返回
    if (job == null) {
      return;
    }
    // 如果指定了任务ID，获取任务对象，收集所有任务尝试的计数器值
    if (taskID != null) {
      task = job.getTask(taskID);
      if (task == null) {
        return;
      }
      // 遍历当前任务的所有尝试，提取指定计数器的值
      for(Map.Entry<TaskAttemptId, TaskAttempt> entry : 
        task.getAttempts().entrySet()) {
        long value = 0;
        Counters counters = entry.getValue().getCounters();
        CounterGroup group = (counters != null) ? counters
          .getGroup($(COUNTER_GROUP)) : null;
        if(group != null)  {
          Counter c = group.findCounter($(COUNTER_NAME));
          if(c != null) {
            value = c.getValue();
          }
        }
        values.put(MRApps.toString(entry.getKey()), value);
      }
      
      return;
    }
    // 未指定任务ID，收集当前作业下所有符合类型任务的指定计数器值
    Map<TaskId, Task> tasks = job.getTasks();
    for(Map.Entry<TaskId, Task> entry : tasks.entrySet()) {
      long value = 0;
      Counters counters = entry.getValue().getCounters();
      CounterGroup group = (counters != null) ? counters
        .getGroup($(COUNTER_GROUP)) : null;
      if(group != null)  {
        Counter c = group.findCounter($(COUNTER_NAME));
        if(c != null) {
          value = c.getValue();
        }
      }
      // 仅保留任务类型匹配当前页面要求的计数器
      if (counterType == null ||
              counterType == entry.getValue().getType()) {
        values.put(MRApps.toString(entry.getKey()), value);
      }
    }
  }
}