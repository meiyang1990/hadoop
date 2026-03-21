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

package org.apache.hadoop.yarn.server.webapp;

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.util.Log4jWarningErrorMetricsAppender;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * YARN Web UI 错误警告信息展示块，用于在Web页面展示Log4j收集到的系统错误和警告日志统计
 */
public class ErrorsAndWarningsBlock extends HtmlBlock {

  // 统计截止时间戳（秒），只展示该时间戳之后产生的日志
  long cutoffPeriodSeconds;
  final private AdminACLsManager adminAclsManager;

  @Inject
  ErrorsAndWarningsBlock(ViewContext ctx, Configuration conf) {
    super(ctx);
    // 默认展示所有错误和警告，设置截止时间为当前时间
    cutoffPeriodSeconds = Time.now() / 1000;
    String value = ctx.requestContext().get("cutoff", "");
    try {
      cutoffPeriodSeconds = Integer.parseInt(value);
      if (cutoffPeriodSeconds <= 0) {
        // 非法值重置为默认，展示所有日志
        cutoffPeriodSeconds = Time.now() / 1000;
      }
    } catch (NumberFormatException ne) {
      // 格式错误重置为默认，展示所有日志
      cutoffPeriodSeconds = Time.now() / 1000;
    }
    adminAclsManager = new AdminACLsManager(conf);
  }

  @Override
  protected void render(Block html) {
    boolean isAdmin = false;
    UserGroupInformation callerUGI = this.getCallerUGI();

    // 检查ACL是否开启
    if (adminAclsManager.areACLsEnabled()) {
      // 验证当前用户是否为管理员
      if (callerUGI != null && adminAclsManager.isAdmin(callerUGI)) {
        isAdmin = true;
      }
    } else {
      // ACL未开启，默认所有用户均可访问
      isAdmin = true;
    }

    // 非管理员访问提示
    if (!isAdmin) {
      html.div().p().__("This page is for admins only.").__().__();
      return;
    }

    // 仅当使用Log4j日志框架时渲染内容
    if (GenericsUtil.isLog4jLogger(ErrorsAndWarningsBlock.class)) {
      // 渲染错误和警告统计概览块
      html.__(ErrorMetrics.class);
      html.__(WarningMetrics.class);
      // 渲染时间范围选择下拉框和刷新按钮
      html.div().button().$onclick("reloadPage()").b("View data for the last ")
        .__().select().$id("cutoff").option().$value("60").__("1 min").__()
        .option().$value("300").__("5 min").__().option().$value("900")
        .__("15 min").__().option().$value("3600").__("1 hour").__().option()
        .$value("21600").__("6 hours").__().option().$value("43200")
        .__("12 hours").__().option().$value("86400").__("24 hours").__().__().__();

      // 页面刷新JavaScript函数：根据选择的时间范围重新加载页面
      String script = "function reloadPage() {"
          + " var timePeriod = $(\"#cutoff\").val();"
          + " document.location.href = '/cluster/errors-and-warnings?cutoff=' + timePeriod"
          + "}";
      script =  script
          + "; function toggleContent(element) {"
          + "  $(element).parent().siblings('.toggle-content').fadeToggle();"
          + "}";

      // 注入JavaScript代码
      html.script().$type("text/javascript").__(script).__();

      // 设置长消息折叠样式
      html.style(".toggle-content { display: none; }");

      // 获取Log4j错误警告收集器实例
      Log4jWarningErrorMetricsAppender appender =
          Log4jWarningErrorMetricsAppender.findAppender();
      if (appender == null) {
        return;
      }
      List<Long> cutoff = new ArrayList<>();
      // 创建错误消息表格
      Hamlet.TBODY<Hamlet.TABLE<Hamlet>> errorsTable =
          html.table("#messages").thead().tr().th(".message", "Message")
            .th(".type", "Type").th(".count", "Count")
            .th(".lasttime", "Latest Message Time").__().__().tbody();

      // 计算时间截（单位：秒）
      cutoff.add((Time.now() - cutoffPeriodSeconds * 1000) / 1000);
      // 获取错误消息统计数据
      List<Map<String, Log4jWarningErrorMetricsAppender.Element>> errorsData =
          appender.getErrorMessagesAndCounts(cutoff);
      // 获取警告消息统计数据
      List<Map<String, Log4jWarningErrorMetricsAppender.Element>> warningsData =
          appender.getWarningMessagesAndCounts(cutoff);
      // 合并错误和警告数据统一处理
      Map<String, List<Map<String, Log4jWarningErrorMetricsAppender.Element>>> sources =
          new HashMap<>();
      sources.put("Error", errorsData);
      sources.put("Warning", warningsData);

      // 消息最大展示长度，超出则折叠
      int maxDisplayLength = 80;
      // 遍历错误和警告两类数据
      for (Map.Entry<String, List<Map<String, Log4jWarningErrorMetricsAppender.Element>>> source : sources
        .entrySet()) {
        String type = source.getKey();
        List<Map<String, Log4jWarningErrorMetricsAppender.Element>> data =
            source.getValue();
        if (data.size() > 0) {
          Map<String, Log4jWarningErrorMetricsAppender.Element> map = data.get(0);
          // 遍历每个消息条目
          for (Map.Entry<String, Log4jWarningErrorMetricsAppender.Element> entry : map
            .entrySet()) {
            String message = entry.getKey();
            Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet>>> row =
                errorsTable.tr();
            Hamlet.TD<Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet>>>> cell =
                row.td();
            // 长消息或含换行的消息做折叠处理
            if (message.length() > maxDisplayLength || message.contains("\n")) {
              String displayMessage = entry.getKey().split("\n")[0];
              if (displayMessage.length() > maxDisplayLength) {
                displayMessage = displayMessage.substring(0, maxDisplayLength);
              }
              // 生成折叠UI，点击展开完整消息
              cell.pre().a().$href("#").$onclick("toggleContent(this);")
                .$style("white-space: pre").__(displayMessage).__().__().div()
                .$class("toggle-content").pre().__(message).__().__().__();
            } else {
              // 短消息直接展示
              cell.pre().__(message).__().__();
            }
            Log4jWarningErrorMetricsAppender.Element ele = entry.getValue();
            // 填充类型、次数、最新时间列
            row.td(type).td(String.valueOf(ele.count))
              .td(Times.format(ele.timestampSeconds * 1000)).__();
          }
        }
      }
      // 结束表格渲染
      errorsTable.__().__();
    }
  }

  /**
   * 错误警告统计概览基类，按不同时间区间统计总数量并展示
   */
  public static class MetricsBase extends HtmlBlock {
    List<Long> cutoffs;
    List<Integer> values;
    String tableHeading;
    Log4jWarningErrorMetricsAppender appender;

    MetricsBase(ViewContext ctx) {
      super(ctx);
      cutoffs = new ArrayList<>();

      // 预定义1分钟到24小时共7个时间区间，计算每个区间的时间截（单位：秒）
      long now = Time.now();
      cutoffs.add((now - 60 * 1000) / 1000);
      cutoffs.add((now - 300 * 1000) / 1000);
      cutoffs.add((now - 900 * 1000) / 1000);
      cutoffs.add((now - 3600 * 1000) / 1000);
      cutoffs.add((now - 21600 * 1000) / 1000);
      cutoffs.add((now - 43200 * 1000) / 1000);
      cutoffs.add((now - 84600 * 1000) / 1000);

      if (GenericsUtil.isLog4jLogger(ErrorsAndWarningsBlock.class)) {
        appender =
            Log4jWarningErrorMetricsAppender.findAppender();
      }
    }

    /**
     * 获取所有预定义时间区间的时间截
     * @return 时间截列表（单位：秒）
     */
    List<Long> getCutoffs() {
      return this.cutoffs;
    }

    @Override
    protected void render(Block html) {
      if (GenericsUtil.isLog4jLogger(ErrorsAndWarningsBlock.class)) {
        // 渲染统计概览表格
        Hamlet.DIV<Hamlet> div =
            html.div().$class("metrics").$style("padding-bottom: 20px");
        div.h3(tableHeading).table("#metricsoverview").thead()
          .$class("ui-widget-header").tr().th().$class("ui-state-default")
          .__("Last 1 minute").__().th().$class("ui-state-default")
          .__("Last 5 minutes").__().th().$class("ui-state-default")
          .__("Last 15 minutes").__().th().$class("ui-state-default")
          .__("Last 1 hour").__().th().$class("ui-state-default")
          .__("Last 6 hours").__().th().$class("ui-state-default")
          .__("Last 12 hours").__().th().$class("ui-state-default")
          .__("Last 24 hours").__().__().__().tbody().$class("ui-widget-content")
          .tr().td(String.valueOf(values.get(0)))
          .td(String.valueOf(values.get(1))).td(String.valueOf(values.get(2)))
          .td(String.valueOf(values.get(3))).td(String.valueOf(values.get(4)))
          .td(String.valueOf(values.get(5))).td(String.valueOf(values.get(6)))
          .__().__().__();
        div.__();
      }
    }
  }

  /**
   * 错误统计概览子类
   */
  public static class ErrorMetrics extends MetricsBase {

    @Inject
    ErrorMetrics(ViewContext ctx) {
      super(ctx);
      tableHeading = "Error Metrics";
    }

    @Override
    protected void render(Block html) {
      if (appender == null) {
        return;
      }
      // 获取各时间区间错误数量
      values = appender.getErrorCounts(getCutoffs());
      super.render(html);
    }
  }

  /**
   * 警告统计概览子类
   */
  public static class WarningMetrics extends MetricsBase {

    @Inject
    WarningMetrics(ViewContext ctx) {
      super(ctx);
      tableHeading = "Warning Metrics";
    }

    @Override
    protected void render(Block html) {
      if (appender == null) {
        return;
      }
      // 获取各时间区间警告数量
      values = appender.getWarningCounts(getCutoffs());
      super.render(html);
    }
  }
}