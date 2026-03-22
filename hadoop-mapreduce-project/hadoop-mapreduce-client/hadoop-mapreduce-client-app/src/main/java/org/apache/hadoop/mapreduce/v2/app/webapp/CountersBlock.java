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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_TABLE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;

import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TD;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * 任务/作业计数器页面HTML块，负责在MR ApplicationMaster WebUI中渲染作业或任务的统计计数器表格
 */
public class CountersBlock extends HtmlBlock {
  Job job;
  Task task;
  Counters total;
  Counters map;
  Counters reduce;

  /**
   * 构造方法，注入上下文并初始化计数器数据
   * @param appCtx MR应用上下文
   * @param ctx 视图上下文
   */
  @Inject CountersBlock(AppContext appCtx, ViewContext ctx) {
    super(ctx);
    getCounters(appCtx);
  }

  /**
   * 渲染计数器HTML页面块
   * @param html HTML块输出对象
   */
  @Override protected void render(Block html) {
    if (job == null) {
      // 作业不存在，输出错误提示
      html.
        p().__("Sorry, no counters for nonexistent", $(JOB_ID, "job")).__();
      return;
    }
    if (!$(TASK_ID).isEmpty() && task == null) {
      // 任务不存在，输出错误提示
      html.
        p().__("Sorry, no counters for nonexistent", $(TASK_ID, "task")).__();
      return;
    }
    
    if(total == null || total.getGroupNames() == null || total.countCounters() == 0) {
      // 没有计数器数据，输出提示
      String type = $(TASK_ID);
      if(type == null || type.isEmpty()) {
        type = $(JOB_ID, "the job");
      }
      html.
        p().__("Sorry it looks like ", type, " has no counters.").__();
      return;
    }
    
    String urlBase;
    String urlId;
    // 根据当前查看的是任务还是作业生成跳转链接基础路径
    if(task != null) {
      urlBase = "singletaskcounter";
      urlId = MRApps.toString(task.getID());
    } else {
      urlBase = "singlejobcounter";
      urlId = MRApps.toString(job.getID());
    }
    
    
    int numGroups = 0;
    // 创建计数器表格容器和表头
    TBODY<TABLE<DIV<Hamlet>>> tbody = html.
      div(_INFO_WRAP).
      table("#counters").
        thead().
          tr().
            th(".group.ui-state-default", "Counter Group").
            th(".ui-state-default", "Counters").__().__().
        tbody();
    // 遍历所有计数器分组
    for (CounterGroup g : total) {
      // 获取Map和Reduce阶段对应的分组
      CounterGroup mg = map == null ? null : map.getGroup(g.getName());
      CounterGroup rg = reduce == null ? null : reduce.getGroup(g.getName());
      ++numGroups;
      // This is mostly for demonstration :) Typically we'd introduced
      // a CounterGroup block to reduce the verbosity. OTOH, this
      // serves as an indicator of where we're in the tag hierarchy.
      // 创建分组表头行
      TR<THEAD<TABLE<TD<TR<TBODY<TABLE<DIV<Hamlet>>>>>>>> groupHeadRow = tbody.
        tr().
          th().$title(g.getName()).$class("ui-state-default").
          __(fixGroupDisplayName(g.getDisplayName())).__().
          td().$class(C_TABLE).
            table(".dt-counters").$id(job.getID()+"."+g.getName()).
              thead().
                tr().th(".name", "Name");

      // 如果是作业级别，添加Map和Reduce列表头
      if (map != null) {
        groupHeadRow.th("Map").th("Reduce");
      }
      // 添加总值列表头，创建分组表体
      // Ditto
      TBODY<TABLE<TD<TR<TBODY<TABLE<DIV<Hamlet>>>>>>> group = groupHeadRow.
            th(map == null ? "Value" : "Total").__().__().
        tbody();
      // 遍历分组内的所有计数器
      for (Counter counter : g) {
        // Ditto
        // 创建当前计数器行
        TR<TBODY<TABLE<TD<TR<TBODY<TABLE<DIV<Hamlet>>>>>>>> groupRow = group.
          tr();
          // 非任务详情页且无分阶段数据时，直接显示计数器名称；否则添加跳转链接
          if (task == null && mg == null && rg == null) {
            groupRow.td().$title(counter.getName()).__(counter.getDisplayName()).
                __();
          } else {
            groupRow.td().$title(counter.getName()).
              a(url(urlBase,urlId,g.getName(), 
                  counter.getName()), counter.getDisplayName()).
                __();
          }
        // 作业级别，添加Map和Reduce阶段计数值
        if (map != null) {
          Counter mc = mg == null ? null : mg.findCounter(counter.getName());
          Counter rc = rg == null ? null : rg.findCounter(counter.getName());
          groupRow.
            td(mc == null ? "0" : String.format("%,d", mc.getValue())).
            td(rc == null ? "0" : String.format("%,d", rc.getValue()));
        }
        // 添加计数器总值
        groupRow.td(String.format("%,d", counter.getValue())).__();
      }
      // 闭合表格层级标签
      group.__().__().__().__();
    }
    // 闭合表格容器标签
    tbody.__().__().__();
  }

  /**
   * 从应用上下文中获取对应作业/任务的计数器数据，包括分阶段的Map/Reduce计数器
   * @param ctx MR应用上下文
   */
  private void getCounters(AppContext ctx) {
    JobId jobID = null;
    TaskId taskID = null;
    String tid = $(TASK_ID);
    // 解析请求参数中的任务ID
    if (!tid.isEmpty()) {
      taskID = MRApps.toTaskID(tid);
      jobID = taskID.getJobId();
    } else {
      // 解析请求参数中的作业ID
      String jid = $(JOB_ID);
      if (jid != null && !jid.isEmpty()) {
        jobID = MRApps.toJobID(jid);
      }
    }
    if (jobID == null) {
      return;
    }
    // 从上下文中获取作业对象
    job = ctx.getJob(jobID);
    if (job == null) {
      return;
    }
    // 如果是任务级别，获取任务对象和任务计数器
    if (taskID != null) {
      task = job.getTask(taskID);
      if (task == null) {
        return;
      }
      total = task.getCounters();
      return;
    }
    // Get all types of counters
    // 作业级别，聚合所有任务的计数器，拆分Map和Reduce阶段
    Map<TaskId, Task> tasks = job.getTasks();
    total = job.getAllCounters();
    boolean needTotalCounters = false;
    if (total == null) {
      total = new Counters();
      needTotalCounters = true;
    }
    map = new Counters();
    reduce = new Counters();
    // 遍历所有任务，按任务类型累加计数器到对应分组
    for (Task t : tasks.values()) {
      Counters counters = t.getCounters();
      if (counters == null) {
        continue;
      }
      switch (t.getType()) {
        case MAP:     map.incrAllCounters(counters);     break;
        case REDUCE:  reduce.incrAllCounters(counters);  break;
      }
      // 如果作业没有预置的总计数器，则手动累加所有任务计数器得到总值
      if (needTotalCounters) {
        total.incrAllCounters(counters);
      }
    }
  }

  /**
   * 格式化计数器分组显示名称，添加零宽空格实现长名称换行，避免HTML排版溢出
   * @param name 原始分组显示名称
   * @return 格式化后的显示名称
   */
  private String fixGroupDisplayName(CharSequence name) {
    return name.toString().replace(".", ".\u200B").replace("$", "\u200B$");
  }
}