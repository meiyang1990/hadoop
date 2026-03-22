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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.MapTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TFOOT;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * 历史任务服务WebUI中，按任务类型渲染任务列表表格的HTML块
 * 用于展示已完成作业的任务列表及成功尝试的详细信息，供前端DataTables渲染
 */
public class HsTasksBlock extends HtmlBlock {
  final App app;

  /**
   * 通过依赖注入构造任务列表HTML块
   * @param app 历史作业应用上下文对象，包含作业信息
   */
  @Inject HsTasksBlock(App app) {
    this.app = app;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */

  /**
   * 渲染任务列表HTML页面
   * @param html HTML块构建器
   */
  @Override protected void render(Block html) {
    if (app.getJob() == null) {
      // 作业不存在，渲染标题后返回
      html.
        h2($(TITLE));
      return;
    }
    TaskType type = null;
    String symbol = $(TASK_TYPE);
    if (!symbol.isEmpty()) {
      // 从请求参数解析任务类型（MAP/REDUCE）
      type = MRApps.taskType(symbol);
    }
    THEAD<TABLE<Hamlet>> thead;
    if(type != null)
      // 根据作业ID和任务类型生成表格ID
      thead = html.table("#"+app.getJob().getID() 
        + type).$class("dt-tasks").thead();
    else
      // 所有任务共用默认表格ID
      thead = html.table("#tasks").thead();
    // 根据任务类型计算尝试列的跨列数，Reduce任务列数更多
    int attemptColSpan = type == TaskType.REDUCE ? 8 : 3;
    thead.tr().
      th().$colspan(5).$class("ui-state-default").__("Task").__().
      th().$colspan(attemptColSpan).$class("ui-state-default").
        __("Successful Attempt").__().
        __();

    TR<THEAD<TABLE<Hamlet>>> theadRow = thead.
          tr().
            th("Name").
            th("State").
            th("Start Time").
            th("Finish Time").
            th("Elapsed Time").
            th("Start Time"); // 尝试列头

    if(type == TaskType.REDUCE) {
      theadRow.th("Shuffle Finish Time"); // Reduce特有尝试列头
      theadRow.th("Merge Finish Time"); // Reduce特有尝试列头
    }

    theadRow.th("Finish Time"); // 尝试列头

    if(type == TaskType.REDUCE) {
      theadRow.th("Elapsed Time Shuffle"); // Reduce特有耗时列头
      theadRow.th("Elapsed Time Merge"); // Reduce特有耗时列头
      theadRow.th("Elapsed Time Reduce"); // Reduce特有耗时列头
    }
    theadRow.th("Elapsed Time"); // 尝试总耗时列头

    TBODY<TABLE<Hamlet>> tbody = theadRow.__().__().tbody();

    // 将任务数据构造成JavaScript二维数组，供前端JQuery DataTables渲染
    StringBuilder tasksTableData = new StringBuilder("[\n");
    // 遍历作业中所有任务
    for (Task task : app.getJob().getTasks().values()) {
      if (type != null && task.getType() != type) {
        // 过滤不符合当前类型的任务
        continue;
      }
      TaskInfo info = new TaskInfo(task);
      String tid = info.getId();

      long startTime = info.getStartTime();
      long finishTime = info.getFinishTime();
      long elapsed = info.getElapsedTime();

      // 初始化成功尝试各阶段时间数据
      long attemptStartTime = -1;
      long shuffleFinishTime = -1;
      long sortFinishTime = -1;
      long attemptFinishTime = -1;
      long elapsedShuffleTime = -1;
      long elapsedSortTime = -1;
      long elapsedReduceTime = -1;
      long attemptElapsed = -1;
      TaskAttempt successful = info.getSuccessful();
      if(successful != null) {
        // 存在成功尝试，提取各阶段时间
        TaskAttemptInfo ta;
        if(type == TaskType.REDUCE) {
          ReduceTaskAttemptInfo rta = new ReduceTaskAttemptInfo(successful);
          shuffleFinishTime = rta.getShuffleFinishTime();
          sortFinishTime = rta.getMergeFinishTime();
          elapsedShuffleTime = rta.getElapsedShuffleTime();
          elapsedSortTime = rta.getElapsedMergeTime();
          elapsedReduceTime = rta.getElapsedReduceTime();
          ta = rta;
        } else {
          ta = new MapTaskAttemptInfo(successful, false);
        }
        attemptStartTime = ta.getStartTime();
        attemptFinishTime = ta.getFinishTime();
        attemptElapsed = ta.getElapsedTime();
      }
      // 拼接任务数据到JSON数组
      tasksTableData.append("[\"")
      .append("<a href='" + url("task", tid)).append("'>")
      .append(tid).append("</a>\",\"")
      .append(info.getState()).append("\",\"")
      .append(startTime).append("\",\"")
      .append(finishTime).append("\",\"")
      .append(elapsed).append("\",\"")
      .append(attemptStartTime).append("\",\"");

      if(type == TaskType.REDUCE) {
        tasksTableData.append(shuffleFinishTime).append("\",\"")
        .append(sortFinishTime).append("\",\"");
      }
      tasksTableData.append(attemptFinishTime).append("\",\"");
      if(type == TaskType.REDUCE) {
        tasksTableData.append(elapsedShuffleTime).append("\",\"")
        .append(elapsedSortTime).append("\",\"")
        .append(elapsedReduceTime).append("\",\"");
      }
      tasksTableData.append(attemptElapsed).append("\"],\n");
    }
    // 移除最后一个元素末尾多余的逗号
    if(tasksTableData.charAt(tasksTableData.length() - 2) == ',') {
      tasksTableData.delete(
        tasksTableData.length()-2, tasksTableData.length()-1);
    }
    tasksTableData.append("]");
    // 将数据数组输出为JavaScript变量
    html.script().$type("text/javascript").
        __("var tasksTableData=" + tasksTableData).__();
    
    // 构建表格底部搜索输入框行
    TR<TFOOT<TABLE<Hamlet>>> footRow = tbody.__().tfoot().tr();
    footRow.th().input("search_init").$type(InputType.text).$name("task")
        .$value("ID").__().__().th().input("search_init").$type(InputType.text)
        .$name("state").$value("State").__().__().th().input("search_init")
        .$type(InputType.text).$name("start_time").$value("Start Time").__().__()
        .th().input("search_init").$type(InputType.text).$name("finish_time")
        .$value("Finish Time").__().__().th().input("search_init")
        .$type(InputType.text).$name("elapsed_time").$value("Elapsed Time").__()
        .__().th().input("search_init").$type(InputType.text)
        .$name("attempt_start_time").$value("Start Time").__().__();

    if(type == TaskType.REDUCE) {
      // 添加Reduce特有搜索框
      footRow.th().input("search_init").$type(InputType.text)
          .$name("shuffle_time").$value("Shuffle Time").__().__();
      footRow.th().input("search_init").$type(InputType.text)
          .$name("merge_time").$value("Merge Time").__().__();
    }

    footRow.th().input("search_init").$type(InputType.text)
        .$name("attempt_finish").$value("Finish Time").__().__();

    if(type == TaskType.REDUCE) {
      // 添加Reduce特有耗时搜索框
      footRow.th().input("search_init").$type(InputType.text)
          .$name("elapsed_shuffle_time").$value("Elapsed Shuffle Time").__().__();
      footRow.th().input("search_init").$type(InputType.text)
          .$name("elapsed_merge_time").$value("Elapsed Merge Time").__().__();
      footRow.th().input("search_init").$type(InputType.text)
          .$name("elapsed_reduce_time").$value("Elapsed Reduce Time").__().__();
    }

    footRow.th().input("search_init").$type(InputType.text)
        .$name("attempt_elapsed").$value("Elapsed Time").__().__();

    footRow.__().__().__();
  }
}