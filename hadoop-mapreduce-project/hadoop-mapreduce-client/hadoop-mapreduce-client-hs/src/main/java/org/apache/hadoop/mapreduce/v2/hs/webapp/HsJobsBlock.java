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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * 历史服务器WebUI中已完成作业列表页面的渲染块，负责生成历史作业表格的HTML结构和数据。
 * 核心职责：遍历所有已完成历史作业，根据权限过滤，生成前端DataTable可解析的作业数据，输出到HTML页面。
 */
public class HsJobsBlock extends HtmlBlock {
  final AppContext appContext;
  final SimpleDateFormat dateFormat =
    new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");
  private UserGroupInformation ugi;
  private boolean isFilterAppListByUserEnabled;
  private JobACLsManager aclsManager;

  /**
   * 构造函数，通过Guice注入依赖，初始化权限配置和ACL管理器
   * @param conf Hadoop配置对象
   * @param appCtx 历史服务器应用上下文，存储所有已完成作业信息
   * @param ctx Web视图上下文
   */
  @Inject
  HsJobsBlock(Configuration conf, AppContext appCtx, ViewContext ctx) {
    super(ctx);
    appContext = appCtx;
    isFilterAppListByUserEnabled = conf
        .getBoolean(YarnConfiguration.FILTER_ENTITY_LIST_BY_USER, false);
    aclsManager = new JobACLsManager(conf);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */

  /**
   * 渲染已退休作业列表的HTML页面，生成表格结构和作业数据供前端渲染
   * @param html HTML块输出对象
   */
  @Override protected void render(Block html) {
    // 构建表格表头，定义各列名称
    TBODY<TABLE<Hamlet>> tbody = html.
      h2("Retired Jobs").
      table("#jobs").
        thead().
          tr().
            th("Submit Time").
            th("Start Time").
            th("Finish Time").
            th(".id", "Job ID").
            th(".name", "Name").
            th("User").
            th("Queue").
            th(".state", "State").
            th("Maps Total").
            th("Maps Completed").
            th("Reduces Total").
            th("Reduces Completed").
            th("Elapsed Time").__().__().
        tbody();
    LOG.info("Getting list of all Jobs.");
    // 构造JavaScript二维数组，供前端jQuery DataTables渲染表格
    StringBuilder jobsTableData = new StringBuilder("[\n");
    // 遍历所有历史作业
    for (Job j : appContext.getAllJobs().values()) {
      JobInfo job = new JobInfo(j);
      ugi = getCallerUGI();
      // 如果开启按用户过滤应用列表，且当前用户没有该作业的查看权限，则跳过该作业不展示
      if (isFilterAppListByUserEnabled && ugi != null && !aclsManager
          .checkAccess(ugi, JobACL.VIEW_JOB, job.getUserName(), null)) {
        continue;
      }
      // 将作业信息格式化后添加到JSON数组，处理转义和HTML链接
      jobsTableData.append("[\"")
      .append(dateFormat.format(new Date(job.getSubmitTime()))).append("\",\"")
      .append(job.getFormattedStartTimeStr(dateFormat)).append("\",\"")
      .append(dateFormat.format(new Date(job.getFinishTime()))).append("\",\"")
      .append("<a href='").append(url("job", job.getId())).append("'>")
      .append(job.getId()).append("</a>\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        job.getName()))).append("\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        job.getUserName()))).append("\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        job.getQueueName()))).append("\",\"")
      .append(job.getState()).append("\",\"")
      .append(String.valueOf(job.getMapsTotal())).append("\",\"")
      .append(String.valueOf(job.getMapsCompleted())).append("\",\"")
      .append(String.valueOf(job.getReducesTotal())).append("\",\"")
      .append(String.valueOf(job.getReducesCompleted())).append("\",\"")
          .append(
              StringUtils.formatTimeSortable(Times.elapsed(job.getStartTime(),
                  job.getFinishTime(), false))).append("\"],\n");
    }

    // 移除最后一条数据多余的逗号，闭合JSON数组
    if(jobsTableData.charAt(jobsTableData.length() - 2) == ',') {
      jobsTableData.delete(jobsTableData.length()-2, jobsTableData.length()-1);
    }
    jobsTableData.append("]");
    // 将作业数据输出为JavaScript变量，供前端DataTables使用
    html.script().$type("text/javascript").
        __("var jobsTableData=" + jobsTableData).__();
    // 生成表脚搜索输入框，每个列对应一个搜索框
    tbody.__().
    tfoot().
      tr().
        th().input("search_init").$type(InputType.text)
          .$name("submit_time").$value("Submit Time").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("start_time").$value("Start Time").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("finish_time").$value("Finish Time").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("job_id").$value("Job ID").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("name").$value("Name").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("user").$value("User").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("queue").$value("Queue").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("state").$value("State").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("maps_total").$value("Maps Total").__().__().
        th().input("search_init").$type(InputType.text).
          $name("maps_completed").$value("Maps Completed").__().__().
        th().input("search_init").$type(InputType.text).
          $name("reduces_total").$value("Reduces Total").__().__().
        th().input("search_init").$type(InputType.text).
          $name("reduces_completed").$value("Reduces Completed").__().__().
        th().input("search_init").$type(InputType.text).
          $name("elapsed_time").$value("Elapsed Time").__().__().
        __().
        __().
        __();
  }
}