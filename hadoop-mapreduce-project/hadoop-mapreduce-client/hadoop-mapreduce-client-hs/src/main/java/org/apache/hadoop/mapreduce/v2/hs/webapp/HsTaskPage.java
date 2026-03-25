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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.Collection;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.MapTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
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
 * 历史服务器中展示单个任务详细状态信息的页面
 * 核心职责：渲染任务的所有尝试 Attempt 列表及相关状态信息
 */
public class HsTaskPage extends HsView {

  /**
   * 渲染任务尝试列表的HTML块，负责生成任务尝试表格的结构和数据
   */
  static class AttemptsBlock extends HtmlBlock {
    final App app;
    final Configuration conf;

    @Inject
    AttemptsBlock(App ctx, Configuration conf) {
      app = ctx;
      this.conf = conf;
    }

    @Override
    protected void render(Block html) {
      if (!isValidRequest()) {
        // 请求无效，仅渲染标题
        html.
          h2($(TITLE));
        return;
      }
      // 获取任务类型，优先从请求参数获取，否则从任务对象获取
      TaskType type = null;
      String symbol = $(TASK_TYPE);
      if (!symbol.isEmpty()) {
        type = MRApps.taskType(symbol);
      } else {
        type = app.getTask().getType();
      }
      
      // 构建表格表头
      TR<THEAD<TABLE<Hamlet>>> headRow = html.
      table("#attempts").
        thead().
          tr();
      
      headRow.
            th(".id", "Attempt").
            th(".state", "State").
            th(".status", "Status").
            th(".node", "Node").
            th(".logs", "Logs").
            th(".tsh", "Start Time");
      
      // Reduce任务需要额外添加Shuffle和Merge完成时间列
      if(type == TaskType.REDUCE) {
        headRow.th("Shuffle Finish Time");
        headRow.th("Merge Finish Time");
      }
      
      headRow.th("Finish Time");
      
      // Reduce任务需要额外添加各阶段耗时列
      if(type == TaskType.REDUCE) {
        headRow.th("Elapsed Time Shuffle");
        headRow.th("Elapsed Time Merge");
        headRow.th("Elapsed Time Reduce");
      }
      headRow.th("Elapsed Time").
              th(".note", "Note");
      
       TBODY<TABLE<Hamlet>> tbody = headRow.__().__().tbody();
       // 将所有数据写入JavaScript二维数组，供jQuery DataTables渲染
       StringBuilder attemptsTableData = new StringBuilder("[\n");

       // 遍历所有任务尝试，构造表格数据
       for (TaskAttempt attempt : getTaskAttempts()) {
        final TaskAttemptInfo ta = new MapTaskAttemptInfo(attempt, false);
        String taid = ta.getId();

        String nodeHttpAddr = ta.getNode();
        String containerIdString = ta.getAssignedContainerIdStr();
        String nodeIdString = attempt.getAssignedContainerMgrAddress();
        String nodeRackName = ta.getRack();

        // 提取任务尝试各时间点信息
        long attemptStartTime = ta.getStartTime();
        long shuffleFinishTime = -1;
        long sortFinishTime = -1;
        long attemptFinishTime = ta.getFinishTime();
        long elapsedShuffleTime = -1;
        long elapsedSortTime = -1;
        long elapsedReduceTime = -1;
        if(type == TaskType.REDUCE) {
          // Reduce任务计算各阶段耗时
          shuffleFinishTime = attempt.getShuffleFinishTime();
          sortFinishTime = attempt.getSortFinishTime();
          elapsedShuffleTime =
              Times.elapsed(attemptStartTime, shuffleFinishTime, false);
          elapsedSortTime =
              Times.elapsed(shuffleFinishTime, sortFinishTime, false);
          elapsedReduceTime =
              Times.elapsed(sortFinishTime, attemptFinishTime, false); 
        }
        long attemptElapsed =
            Times.elapsed(attemptStartTime, attemptFinishTime, false);
        TaskId taskId = attempt.getID().getTaskId();

        // 将当前尝试数据拼接为JSON数组格式
        attemptsTableData.append("[\"")
        .append(getAttemptId(taskId, ta)).append("\",\"")
        .append(ta.getState()).append("\",\"")
        .append(StringEscapeUtils.escapeEcmaScript(
              StringEscapeUtils.escapeHtml4(ta.getStatus()))).append("\",\"")

        .append("<a class='nodelink' href='" + MRWebAppUtil.getYARNWebappScheme() + nodeHttpAddr + "'>")
        .append(nodeRackName + "/" + nodeHttpAddr + "</a>\",\"");

         // 构造日志链接，根据日志聚合是否开启选择不同链接地址
         String logsUrl = url("logs", nodeIdString, containerIdString, taid,
             app.getJob().getUserName);
         if (!conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
             YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
           // 未开启日志聚合，跳转到NodeManager节点日志
           logsUrl =
               url(MRWebAppUtil.getYARNWebappScheme(), nodeHttpAddr, "node",
                   "containerlogs", containerIdString,
                   app.getJob().getUserName);
         }
         attemptsTableData.append("<a class='logslink' href='").append(logsUrl)
             .append("'>logs</a>\",\"");

        attemptsTableData.append(attemptStartTime).append("\",\"");

        if(type == TaskType.REDUCE) {
          attemptsTableData.append(shuffleFinishTime).append("\",\"")
          .append(sortFinishTime).append("\",\"");
        }
        attemptsTableData.append(attemptFinishTime).append("\",\"");

        if(type == TaskType.REDUCE) {
          attemptsTableData.append(elapsedShuffleTime).append("\",\"")
          .append(elapsedSortTime).append("\",\"")
          .append(elapsedReduceTime).append("\",\"");
        }
          attemptsTableData.append(attemptElapsed).append("\",\"")
          .append(StringEscapeUtils.escapeEcmaScript(
              StringEscapeUtils.escapeHtml4(ta.getNote())))
          .append("\"],\n");
      }
       // 移除最后一个多余的逗号，闭合二维数组
       if(attemptsTableData.charAt(attemptsTableData.length() - 2) == ',') {
         attemptsTableData.delete(attemptsTableData.length()-2, attemptsTableData.length()-1);
       }
       attemptsTableData.append("]");
       // 将数据数组输出到页面脚本中
       html.script().$type("text/javascript").
           __("var attemptsTableData=" + attemptsTableData).__();

      // 构建表尾搜索框，供每列过滤搜索
      TR<TFOOT<TABLE<Hamlet>>> footRow = tbody.__().tfoot().tr();
      footRow.
          th().input("search_init").$type(InputType.text).
              $name("attempt_name").$value("Attempt").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_state").$value("State").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_status").$value("Status").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_node").$value("Node").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_node").$value("Logs").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_start_time").$value("Start Time").__().__();
      
      if(type == TaskType.REDUCE) {
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("shuffle_time").$value("Shuffle Time").__().__();
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("merge_time").$value("Merge Time").__().__();
      }
      
      footRow.
        th().input("search_init").$type(InputType.text).
            $name("attempt_finish").$value("Finish Time").__().__();
      
      if(type == TaskType.REDUCE) {
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("elapsed_shuffle_time").$value("Elapsed Shuffle Time").__().__();
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("elapsed_merge_time").$value("Elapsed Merge Time").__().__();
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("elapsed_reduce_time").$value("Elapsed Reduce Time").__().__();
      }

      footRow.
        th().input("search_init").$type(InputType.text).
            $name("attempt_elapsed").$value("Elapsed Time").__().__().
        th().input("search_init").$type(InputType.text).
            $name("note").$value("Note").__().__();
      
      footRow.__().__().__();
    }

    protected String getAttemptId(TaskId taskId, TaskAttemptInfo ta) {
      return ta.getId();
    }

    /**
     * 校验请求是否合法，检查任务对象是否存在
     * @return true 合法请求，false 非法请求
     */
    protected boolean isValidRequest() {
      return app.getTask() != null;
    }

    /**
     * 获取当前任务的所有尝试列表
     * @return 任务尝试集合
     */
    protected Collection<TaskAttempt> getTaskAttempts() {
      return app.getTask().getAttempts().values();
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 在HTML head部分预先加载所需资源和初始化配置
   * @param html HTML对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    // 覆盖导航手风琴配置，默认展开第三个菜单
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:2}");
    // 配置尝试列表DataTables相关参数
    set(DATATABLES_ID, "attempts");
    set(initID(DATATABLES, "attempts"), attemptsTableInit());
    set(postInitID(DATATABLES, "attempts"), attemptsPostTableInit());
    setTableStyles(html, "attempts");
  }

  /**
   * 获取页面内容区块类型
   * @return 尝试列表区块类
   */
  @Override protected Class<? extends SubView> content() {
    return AttemptsBlock.class;
  }

  /**
   * 生成DataTables配置初始化JS代码，根据任务类型调整列配置
   * @return DataTables初始化JSON字符串
   */
  private String attemptsTableInit() {
    TaskType type = null;
    String symbol = $(TASK_TYPE);
    if (!symbol.isEmpty()) {
      type = MRApps.taskType(symbol);
    } else {
      TaskId taskID = MRApps.toTaskID($(TASK_ID));
      type = taskID.getTaskType();
    }
    StringBuilder b = tableInit()
      .append(", 'aaData': attemptsTableData")
      .append(", bDeferRender: true")
      .append(", bProcessing: true")
      .append("\n,aoColumnDefs:[\n")

      // 日志列不需要搜索，排除搜索范围避免容器ID干扰搜索结果
      .append("\n{'aTargets': [ 4 ]")
      .append(", 'bSearchable': false }")

      .append("\n, {'sType':'natural', 'aTargets': [ 0 ]")
      .append(", 'mRender': parseHadoopID }")

      .append("\n, {'sType':'numeric', 'aTargets': [ 5, 6")
      // Map和Reduce任务列数不同，根据任务类型添加额外列
      .append(type == TaskType.REDUCE ? ", 7, 8" : "")
      .append(" ], 'mRender': renderHadoopDate }")

      .append("\n, {'sType':'numeric', 'aTargets': [")
      .append(type == TaskType.REDUCE ? "9, 10, 11, 12" : "7")
      .append(" ], 'mRender': renderHadoopElapsedTime }]")

      // 页面加载完成后默认按尝试ID升序排序
      .append("\n, aaSorting: [[0, 'asc']]")
      .append("}");
      return b.toString();
  }

  /**
   * 生成DataTables初始化后绑定搜索事件的JS代码
   * @return 初始化后处理JS代码字符串
   */
  private String attemptsPostTableInit() {
    return "var asInitVals = new Array();\n" +
           "$('tfoot input').keyup( function () \n{"+
           "  attemptsDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n"+
           "} );\n"+
           "$('tfoot input').each( function (i) {\n"+
           "  asInitVals[i] = this.value;\n"+
           "} );\n"+
           "$('tfoot input').focus( function () {\n"+
           "  if ( this.className == 'search_init' )\n"+
           "  {\n"+
           "    this.className = '';\n"+
           "    this.value = '';\n"+
           "  }\n"+
           "} );\n"+
           "$('tfoot input').blur( function (i) {\n"+
           "  if ( this.value == '' )\n"+
           "  {\n"+
           "    this.className = 'search_init';\n"+
           "    this.value = asInitVals[$('tfoot input').index(this)];\n"+
           "  }\n"+
           "} );\n";
  }
}