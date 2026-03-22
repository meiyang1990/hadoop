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

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR_VALUE;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * MapReduce Application Web UI 作业列表块渲染类
 * 负责在应用页面渲染当前所有活跃作业的信息表格，展示作业基本信息和执行进度
 */
public class JobsBlock extends HtmlBlock {
  final AppContext appContext;

  /**
   * 构造方法，通过依赖注入获取应用上下文
   * @param appCtx MapReduce应用上下文，包含所有作业信息
   */
  @Inject JobsBlock(AppContext appCtx) {
    appContext = appCtx;
  }

  /**
   * 渲染作业列表HTML块，生成活跃作业信息表格
   * @param html HTML块构建对象
   */
  @Override protected void render(Block html) {
    // 构建表格表头，定义各列含义
    TBODY<TABLE<Hamlet>> tbody = html.
      h2("Active Jobs").
      table("#jobs").
        thead().
          tr().
            th(".id", "Job ID").
            th(".name", "Name").
            th(".state", "State").
            th("Map Progress").
            th("Maps Total").
            th("Maps Completed").
            th("Reduce Progress").
            th("Reduces Total").
            th("Reduces Completed").__().__().
        tbody();
    // 遍历所有作业，逐行渲染作业信息
    for (Job j : appContext.getAllJobs().values()) {
      JobInfo job = new JobInfo(j, false);
      tbody.
        tr().
          td().
            span().$title(String.valueOf(job.getId())).__(). // 用于前端排序
            a(url("job", job.getId()), job.getId()).__(). // 生成作业详情链接
          td(job.getName()).
          td(job.getState()).
          td().
            span().$title(job.getMapProgressPercent()).__(). // 用于前端排序
            div(_PROGRESSBAR).
              $title(join(job.getMapProgressPercent(), '%')). // 进度提示浮窗
              div(_PROGRESSBAR_VALUE).
                $style(join("width:", job.getMapProgressPercent(), '%')).__().__().__().
          td(String.valueOf(job.getMapsTotal())).
          td(String.valueOf(job.getMapsCompleted())).
          td().
            span().$title(job.getReduceProgressPercent()).__(). // 用于前端排序
            div(_PROGRESSBAR).
              $title(join(job.getReduceProgressPercent(), '%')). // 进度提示浮窗
              div(_PROGRESSBAR_VALUE).
                $style(join("width:", job.getReduceProgressPercent(), '%')).__().__().__().
          td(String.valueOf(job.getReducesTotal())).
          td(String.valueOf(job.getReducesCompleted())).__();
    }
    tbody.__().__();
  }
}