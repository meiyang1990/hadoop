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
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfEntryInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * 作业配置信息HTML渲染块，在MR ApplicationMaster WebUI中展示指定作业的完整配置项
 * 负责将作业配置数据转换为HTML表格，提供配置键、值和来源链路展示，支持下载原始配置文件
 */
public class ConfBlock extends HtmlBlock {
  final AppContext appContext;

  /**
   * 构造函数，注入应用上下文依赖
   * @param appctx MR ApplicationMaster应用上下文，用于获取作业信息
   */
  @Inject ConfBlock(AppContext appctx) {
    appContext = appctx;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */

  /**
   * 渲染作业配置信息HTML页面
   * @param html HTML块输出对象
   */
  @Override protected void render(Block html) {
    // 从请求中获取作业ID参数
    String jid = $(JOB_ID);
    if (jid.isEmpty()) {
      // 无作业ID时输出错误信息
      html.
        p().__("Sorry, can't do anything without a JobID.").__();
      return;
    }
    // 将字符串作业ID转换为JobId对象
    JobId jobID = MRApps.toJobID(jid);
    // 从应用上下文获取对应作业对象
    Job job = appContext.getJob(jobID);
    if (job == null) {
      // 作业不存在时输出错误信息
      html.
        p().__("Sorry, ", jid, " not found.").__();
      return;
    }
    // 获取作业配置文件路径
    Path confPath = job.getConfFile();
    try {
      // 构造作业配置信息对象，加载所有配置项
      ConfInfo info = new ConfInfo(job);

      // 添加配置文件下载链接
      html.div().a("/jobhistory/downloadconf/" + jid, confPath.toString()).__();
      // 创建配置表格DOM结构，初始化表头
      TBODY<TABLE<Hamlet>> tbody = html.
        table("#conf").
        thead().
          tr().
            th(_TH, "key").
            th(_TH, "value").
            th(_TH, "source chain").
              __().
              __().
      tbody();
      // 遍历所有配置项，逐行渲染表格内容
      for (ConfEntryInfo entry : info.getProperties()) {
        // 拼接配置来源链路字符串
        StringBuilder buffer = new StringBuilder();
        String[] sources = entry.getSource();
        //Skip the last entry, because it is always the same HDFS file, and
        // output them in reverse order so most recent is output first
        // 跳过最后一个HDFS文件来源，倒序输出保证最新来源优先展示
        boolean first = true;
        for(int i = (sources.length  - 2); i >= 0; i--) {
          if(!first) {
            buffer.append(" <- ");
          }
          first = false;
          buffer.append(sources[i]);
        }
        // 添加当前配置项到表格行
        tbody.
          tr().
            td(entry.getName()).
            td(entry.getValue()).
            td(buffer.toString()).
            __();
      }
      // 完成表格渲染，添加搜索栏表头脚
      tbody.__().
      tfoot().
        tr().
          th().input("search_init").$type(InputType.text).$name("key").$value("key").__().__().
          th().input("search_init").$type(InputType.text).$name("value").$value("value").__().__().
          th().input("search_init").$type(InputType.text).$name("source chain").$value("source chain").__().__().
          __().
          __().
          __();
    } catch(IOException e) {
      // 读取配置文件异常处理，输出错误信息
      LOG.error("Error while reading "+confPath, e);
      html.p().__("Sorry got an error while reading conf file. ", confPath).__();
    }
  }
}