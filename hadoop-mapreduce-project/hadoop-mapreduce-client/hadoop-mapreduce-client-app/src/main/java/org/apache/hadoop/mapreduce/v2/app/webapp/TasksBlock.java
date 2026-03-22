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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_STATE;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * 任务列表页面HTML块，负责在MapReduce ApplicationMaster WebUI中渲染任务列表表格
 * 核心职责：根据过滤条件（任务类型、任务状态）筛选任务，构造前端表格所需的JSON数据，生成任务列表HTML结构
 */
public class TasksBlock extends HtmlBlock {
  final App app;

  @Inject 
  /**
   * 构造函数，通过依赖注入获取Application上下文
   * @param app Application上下文对象，包含当前作业信息
   */
  TasksBlock(App app) {
    this.app = app;
  }

  /**
   * 渲染任务列表HTML块
   * @param html HTML块输出对象
   */
  @Override protected void render(Block html) {
    // 当前作业不存在时，仅渲染标题
    if (app.getJob() == null) {
      html.
        h2($(TITLE));
      return;
    }
    TaskType type = null;
    String symbol = $(TASK_TYPE);
    // 从请求参数解析任务类型过滤条件
    if (!symbol.isEmpty()) {
      type = MRApps.taskType(symbol);
    }
    // 创建任务列表表格框架并渲染表头
    TBODY<TABLE<Hamlet>> tbody = html.
      table("#tasks").
        thead().
          tr().
            th("Task").
            th("Progress").
            th("Status").
            th("State").
            th("Start Time").
            th("Finish Time").
            th("Elapsed Time").__().__().
        tbody();
    // 构建前端表格所需的JSON数据数组
    StringBuilder tasksTableData = new StringBuilder("[\n");

    // 遍历当前作业所有任务，按过滤条件筛选并构造表格数据
    for (Task task : app.getJob().getTasks().values()) {
      // 按任务类型过滤，不符合则跳过
      if (type != null && task.getType() != type) {
        continue;
      }
      String taskStateStr = $(TASK_STATE);
      // 未指定状态过滤条件时默认显示所有状态
      if (taskStateStr == null || taskStateStr.trim().equals("")) {
        taskStateStr = "ALL";
      }

      // 按任务状态过滤
      if (!taskStateStr.equalsIgnoreCase("ALL"))
      {
        try {
          // 解析状态过滤条件
          MRApps.TaskStateUI stateUI = MRApps.taskState(taskStateStr);
          // 当前任务状态不匹配则跳过
          if (!stateUI.correspondsTo(task.getState()))
          {
            continue;
          }
        } catch (IllegalArgumentException e) {
          continue; // 非法状态值，跳过该任务
        }
      }

      // 构造任务信息数据对象
      TaskInfo info = new TaskInfo(task);
      String tid = info.getId();
      String pct = StringUtils.format("%.2f", info.getProgress());
      // 将任务信息拼接为JSON数组项，包含HTML链接和进度条
      tasksTableData.append("[\"<a href='").append(url("task", tid))
      .append("'>").append(tid).append("</a>\",\"")
      // 拼接进度条HTML
      .append("<br title='").append(pct)
      .append("'> <div class='").append(C_PROGRESSBAR).append("' title='")
      .append(join(pct, '%')).append("'> ").append("<div class='")
      .append(C_PROGRESSBAR_VALUE).append("' style='")
      .append(join("width:", pct, '%')).append("'> </div> </div>\",\"")
      // 转义任务状态文本防止XSS和JSON解析错误
      .append(StringEscapeUtils.escapeEcmaScript(
              StringEscapeUtils.escapeHtml4(info.getStatus()))).append("\",\"")

      .append(info.getState()).append("\",\"")
      .append(info.getStartTime()).append("\",\"")
      .append(info.getFinishTime()).append("\",\"")
      .append(info.getElapsedTime()).append("\"],\n");
    }
    // 移除最后一项多余的逗号，保证JSON格式合法
    if(tasksTableData.charAt(tasksTableData.length() - 2) == ',') {
      tasksTableData.delete(tasksTableData.length()-2, tasksTableData.length()-1);
    }
    // 关闭JSON数组
    tasksTableData.append("]");
    // 将JSON数据写入页面JavaScript变量，供前端表格渲染使用
    html.script().$type("text/javascript").
        __("var tasksTableData=" + tasksTableData).__();

    // 关闭表格标签
    tbody.__().__();
  }
}