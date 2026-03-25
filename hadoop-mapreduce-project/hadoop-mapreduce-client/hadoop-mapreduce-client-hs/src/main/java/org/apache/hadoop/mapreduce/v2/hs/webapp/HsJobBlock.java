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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfEntryInfo;
import org.apache.hadoop.mapreduce.v2.hs.UnparsedJob;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * 历史服务器作业详情页面HTML块，负责渲染单个已完成作业的概览信息
 * 展示作业基本信息、ApplicationMaster信息、任务统计、尝试统计等内容
 */
public class HsJobBlock extends HtmlBlock {
  final AppContext appContext;

  /**
   * 构造方法，注入应用上下文
   * @param appctx 历史服务器应用上下文
   */
  @Inject HsJobBlock(AppContext appctx) {
    appContext = appctx;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */
  /**
   * 渲染作业详情HTML页面块
   * @param html HTML构建对象
   */
  @Override protected void render(Block html) {
    String jid = $(JOB_ID);
    if (jid.isEmpty()) {
      html.
        p().__("Sorry, can't do anything without a JobID.").__();
      return;
    }
    // 转换字符串JobID为JobId对象
    JobId jobID = MRApps.toJobID(jid);
    // 从上下文中获取作业对象
    Job j = appContext.getJob(jobID);
    if (j == null) {
      html.p().__("Sorry, ", jid, " not found.").__();
      return;
    }
    // 处理超过任务数量限制未被解析的超大作业
    if(j instanceof UnparsedJob) {
      final int taskCount = j.getTotalMaps() + j.getTotalReduces();
      UnparsedJob oversizedJob = (UnparsedJob) j;
      html.p().__("The job has a total of " + taskCount + " tasks. ")
          .__("Any job larger than " + oversizedJob.getMaxTasksAllowed() +
              " will not be loaded.").__();
      html.p().__("You can either use the CLI tool: 'mapred job -history'"
          + " to view large jobs or adjust the property " +
          JHAdminConfig.MR_HS_LOADED_JOBS_TASKS_MAX + ".").__();
      return;
    }
    // 获取作业的所有ApplicationMaster信息
    List<AMInfo> amInfos = j.getAMInfos();
    // 构建作业信息DAO对象
    JobInfo job = new JobInfo(j);
    // 构建作业概览信息块
    ResponseInfo infoBlock = info("Job Overview").
        __("Job Name:", job.getName()).
        __("User Name:", job.getUserName()).
        __("Queue:", job.getQueueName()).
        __("State:", job.getState()).
        __("Uberized:", job.isUber()).
        __("Submitted:", new Date(job.getSubmitTime())).
        __("Started:", job.getStartTimeStr()).
        __("Finished:", new Date(job.getFinishTime())).
        __("Elapsed:", StringUtils.formatTime(
            Times.elapsed(job.getStartTime(), job.getFinishTime(), false)));
    
    // 根据AM数量选择单复数描述
    String amString =
        amInfos.size() == 1 ? "ApplicationMaster" : "ApplicationMasters"; 
    
    // todo - switch to use JobInfo
    // 添加诊断信息到概览，将TaskID文本转换为可点击链接
    List<String> diagnostics = j.getDiagnostics();
    if(diagnostics != null && !diagnostics.isEmpty()) {
      StringBuilder b = new StringBuilder();
      for(String diag: diagnostics) {
        b.append(addTaskLinks(diag));
      }
      infoBlock._r("Diagnostics:", b.toString());
    }

    // 添加Map任务平均时间统计
    if(job.getNumMaps() > 0) {
      infoBlock.__("Average Map Time", StringUtils.formatTime(job.getAvgMapTime()));
    }
    // 添加Reduce任务各阶段平均时间统计
    if(job.getNumReduces() > 0) {
      infoBlock.__("Average Shuffle Time", StringUtils.formatTime(job.getAvgShuffleTime()));
      infoBlock.__("Average Merge Time", StringUtils.formatTime(job.getAvgMergeTime()));
      infoBlock.__("Average Reduce Time", StringUtils.formatTime(job.getAvgReduceTime()));
    }

    // 添加权限ACL信息
    for (ConfEntryInfo entry : job.getAcls()) {
      infoBlock.__("ACL "+entry.getName()+":", entry.getValue());
    }
    // 开始渲染HTML容器
    DIV<Hamlet> div = html.
        __(InfoBlock.class).
      div(_INFO_WRAP);
    
      // 渲染ApplicationMaster信息表格
        TABLE<DIV<Hamlet>> table = div.table("#job");
        table.
          tr().
            th(amString).
            __().
          tr().
            th(_TH, "Attempt Number").
            th(_TH, "Start Time").
            th(_TH, "Node").
            th(_TH, "Logs").
            __();
        boolean odd = false;
        // 遍历所有AM尝试，逐行渲染
          for (AMInfo amInfo : amInfos) {
            AMAttemptInfo attempt = new AMAttemptInfo(amInfo,
                job.getId(), job.getUserName(), "", "");
            // 奇偶行使用不同样式
            table.tr((odd = !odd) ? _ODD : _EVEN).
              td(String.valueOf(attempt.getAttemptId())).
              td(new Date(attempt.getStartTime()).toString()).
              td().a(".nodelink", url(MRWebAppUtil.getYARNWebappScheme(),
                  attempt.getNodeHttpAddress()),
                  attempt.getNodeHttpAddress()).__().
              td().a(".logslink", url(attempt.getLogsLink()),
                      "logs").__().
                __();
          }
          table.__();
          div.__();
          
        
        html.div(_INFO_WRAP).        
      
      // 渲染任务数量统计表格
        table("#job").
          tr().
            th(_TH, "Task Type").
            th(_TH, "Total").
            th(_TH, "Complete").__().
          tr(_ODD).
            th().
              a(url("tasks", jid, "m"), "Map").__().
            td(String.valueOf(String.valueOf(job.getMapsTotal()))).
            td(String.valueOf(String.valueOf(job.getMapsCompleted()))).__().
          tr(_EVEN).
            th().
              a(url("tasks", jid, "r"), "Reduce").__().
            td(String.valueOf(String.valueOf(job.getReducesTotal()))).
            td(String.valueOf(String.valueOf(job.getReducesCompleted()))).__()
          .__().

        // 渲染任务尝试状态统计表格
        table("#job").
        tr().
          th(_TH, "Attempt Type").
          th(_TH, "Failed").
          th(_TH, "Killed").
          th(_TH, "Successful").__().
        tr(_ODD).
          th("Maps").
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.FAILED.toString()), 
              String.valueOf(job.getFailedMapAttempts())).__().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.KILLED.toString()), 
              String.valueOf(job.getKilledMapAttempts())).__().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.SUCCESSFUL.toString()), 
              String.valueOf(job.getSuccessfulMapAttempts())).__().
            __().
        tr(_EVEN).
          th("Reduces").
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.FAILED.toString()), 
              String.valueOf(job.getFailedReduceAttempts())).__().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.KILLED.toString()), 
              String.valueOf(job.getKilledReduceAttempts())).__().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.SUCCESSFUL.toString()), 
              String.valueOf(job.getSuccessfulReduceAttempts())).__().
            __().
            __().
            __();
  }

  /**
   * 将诊断文本中的TaskID替换为指向任务详情页的超链接
   * @param text 原始诊断文本
   * @return 替换后包含超链接的文本
   */
  static String addTaskLinks(String text) {
    return TaskID.taskIdPattern.matcher(text).replaceAll(
        "<a href=\"/jobhistory/task/$0\">$0</a>");
  }
}