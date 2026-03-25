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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;

import java.util.Collection;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.LI;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

/**
 * YARN FairScheduler调度器Web页面渲染类
 * 负责生成公平调度器队列信息和应用信息的Web界面
 */
public class FairSchedulerPage extends RmView {
  // 队列条目的CSS类名
  static final String _Q = ".ui-state-default.ui-corner-all";
  // 队列最大宽度占比
  static final float Q_MAX_WIDTH = 0.8f;
  // 队列统计信息的左侧位置占比
  static final float Q_STATS_POS = Q_MAX_WIDTH + 0.05f;
  // 队列结束位置样式
  static final String Q_END = "left:101%";
  // 稳态公平份额条样式
  static final String Q_GIVEN =
      "left:0%;background:none;border:1px solid #000000";
  // 瞬时公平份额条样式
  static final String Q_INSTANTANEOUS_FS =
      "left:0%;background:none;border:1px dashed #000000";
  // 超过公平份额的背景色
  static final String Q_OVER = "background:#FFA333";
  // 低于公平份额的背景色
  static final String Q_UNDER = "background:#5BD75B";
  // 稳态公平份额常量
  static final String STEADY_FAIR_SHARE = "Steady Fair Share";
  // 瞬时公平份额常量
  static final String INSTANTANEOUS_FAIR_SHARE = "Instantaneous Fair Share";
  
  /**
   * 请求级队列信息存储容器，用于在各个Block之间传递当前队列信息
   */
  @RequestScoped
  static class FSQInfo {
    FairSchedulerQueueInfo qinfo;
  }
  
  /**
   * 叶子队列信息区块渲染类，负责渲染叶子队列的详细状态信息
   */
  static class LeafQueueBlock extends HtmlBlock {
    final FairSchedulerLeafQueueInfo qinfo;

    @Inject LeafQueueBlock(ViewContext ctx, FSQInfo info) {
      super(ctx);
      qinfo = (FairSchedulerLeafQueueInfo)info.qinfo;
    }

    @Override
    protected void render(Block html) {
      // 构建队列状态响应信息
      ResponseInfo ri = info("\'" + qinfo.getQueueName() + "\' Queue Status").
          __("Used Resources:", qinfo.getUsedResources().toString()).
          __("Demand Resources:", qinfo.getDemandResources().toString()).
          __("AM Used Resources:", qinfo.getAMUsedResources().toString()).
          __("AM Max Resources:", qinfo.getAMMaxResources().toString()).
          __("Num Active Applications:", qinfo.getNumActiveApplications()).
          __("Num Pending Applications:", qinfo.getNumPendingApplications()).
          __("Min Resources:", qinfo.getMinResources().toString()).
          __("Max Resources:", qinfo.getMaxResources().toString()).
          __("Max Container Allocation:",
              qinfo.getMaxContainerAllocation().toString()).
          __("Reserved Resources:", qinfo.getReservedResources().toString());
      int maxApps = qinfo.getMaxApplications();
      // 只有配置了最大运行应用数才显示该字段
      if (maxApps < Integer.MAX_VALUE) {
        ri.__("Max Running Applications:", qinfo.getMaxApplications());
      }
      ri.__(STEADY_FAIR_SHARE + ":", qinfo.getSteadyFairShare().toString());
      ri.__(INSTANTANEOUS_FAIR_SHARE + ":", qinfo.getFairShare().toString());
      ri.__("Preemptable:", qinfo.isPreemptable());
      // 渲染信息区块
      html.__(InfoBlock.class);

      // 清空信息内容，避免当前队列信息污染下一个队列
      ri.clear();
    }
  }
  
  /**
   * 父队列信息区块渲染类，负责渲染父队列的状态信息
   */
  static class ParentQueueBlock extends HtmlBlock {
	    final FairSchedulerQueueInfo qinfo;

    @Inject ParentQueueBlock(ViewContext ctx, FSQInfo info) {
      super(ctx);
      qinfo = (FairSchedulerQueueInfo)info.qinfo;
    }

    @Override
    protected void render(Block html) {
      // 构建父队列状态响应信息
      ResponseInfo ri = info("\'" + qinfo.getQueueName() + "\' Queue Status").
          __("Used Resources:", qinfo.getUsedResources().toString()).
          __("Min Resources:", qinfo.getMinResources().toString()).
          __("Max Resources:", qinfo.getMaxResources().toString()).
          __("Max Container Allocation:",
              qinfo.getMaxContainerAllocation().toString()).
          __("Reserved Resources:", qinfo.getReservedResources().toString());
      int maxApps = qinfo.getMaxApplications();
      // 只有配置了最大运行应用数才显示该字段
      if (maxApps < Integer.MAX_VALUE) {
        ri.__("Max Running Applications:", qinfo.getMaxApplications());
      }
      ri.__(STEADY_FAIR_SHARE + ":", qinfo.getSteadyFairShare().toString());
      ri.__(INSTANTANEOUS_FAIR_SHARE + ":", qinfo.getFairShare().toString());
      // 渲染信息区块
      html.__(InfoBlock.class);

      // 清空信息内容，避免当前队列信息污染下一个队列
      ri.clear();
    }
  }

  /**
   * 队列层级结构渲染类，递归渲染所有子队列
   */
  static class QueueBlock extends HtmlBlock {
    final FSQInfo fsqinfo;

    @Inject QueueBlock(FSQInfo info) {
      fsqinfo = info;
    }

    @Override
    public void render(Block html) {
      // 获取当前队列的所有子队列
      Collection<FairSchedulerQueueInfo> subQueues = fsqinfo.qinfo.getChildQueues();
      UL<Hamlet> ul = html.ul("#pq");
      // 遍历每个子队列生成HTML
      for (FairSchedulerQueueInfo info : subQueues) {
        // 获取队列各项资源占比数据
        float capacity = info.getMaxResourcesFraction();
        float steadyFairShare = info.getSteadyFairShareMemoryFraction();
        float instantaneousFairShare = info.getFairShareMemoryFraction();
        float used = info.getUsedMemoryFraction();
        // 创建队列条目，渲染资源占比条
        LI<UL<Hamlet>> li = ul.
          li().
            a(_Q).$style(width(capacity * Q_MAX_WIDTH)).
              $title(join(join(STEADY_FAIR_SHARE + ":", percent(steadyFairShare)),
                  join(" " + INSTANTANEOUS_FAIR_SHARE + ":", percent(instantaneousFairShare)))).
              span().$style(join(Q_GIVEN, ";font-size:1px;", width(steadyFairShare / capacity))).
            __('.').__().
              span().$style(join(Q_INSTANTANEOUS_FS, ";font-size:1px;",
                  width(instantaneousFairShare/capacity))).
            __('.').__().
              span().$style(join(width(used/capacity),
                ";font-size:1px;left:0%;", used > instantaneousFairShare ? Q_OVER : Q_UNDER)).
            __('.').__().
              span(".q", info.getQueueName()).__().
            span().$class("qstats").$style(left(Q_STATS_POS)).
            __(join(percent(used), " used")).__();

        // 更新当前队列信息到容器
        fsqinfo.qinfo = info;
        // 根据队列类型渲染不同区块，叶子队列直接显示详细信息，父队列递归渲染子队列
        if (info instanceof FairSchedulerLeafQueueInfo) {
          li.ul("#lq").li().__(LeafQueueBlock.class).__().__();
        } else {
          li.ul("#lq").li().__(ParentQueueBlock.class).__().__();
          li.__(QueueBlock.class);
        }
        li.__();
      }

      ul.__();
    }
  }
  
  /**
   * 整个调度器队列总区块渲染类，负责渲染根队列和图例
   */
  static class QueuesBlock extends HtmlBlock {
    final FairScheduler fs;
    final FSQInfo fsqinfo;
    
    @Inject QueuesBlock(ResourceManager rm, FSQInfo info) {
      fs = (FairScheduler)rm.getResourceScheduler();
      fsqinfo = info;
    }

    @Override
    public void render(Block html) {
      // 先渲染指标概览表格
      html.__(MetricsOverviewTable.class);
      // 创建应用队列容器DOM结构
      UL<DIV<DIV<Hamlet>>> ul = html.
        div("#cs-wrapper.ui-widget").
          div(".ui-widget-header.ui-corner-top").
          __("Application Queues").__().
          div("#cs.ui-widget-content.ui-corner-bottom").
            ul();
      // 调度器未初始化时显示默认队列
      if (fs == null) {
        ul.
          li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(Q_END).__("100% ").__().
              span(".q", "default").__().__();
      } else {
        // 构建调度器信息对象，获取根队列
        FairSchedulerInfo sinfo = new FairSchedulerInfo(fs);
        fsqinfo.qinfo = sinfo.getRootQueueInfo();
        float used = fsqinfo.qinfo.getUsedMemoryFraction();

        // 渲染图例说明
        ul.
          li().$style("margin-bottom: 1em").
            span().$style("font-weight: bold").__("Legend:").__().
            span().$class("qlegend ui-corner-all").$style(Q_GIVEN).
              $title("The steady fair shares consider all queues, " +
                  "both active (with running applications) and inactive.").
            __(STEADY_FAIR_SHARE).__().
            span().$class("qlegend ui-corner-all").$style(Q_INSTANTANEOUS_FS).
              $title("The instantaneous fair shares consider only active " +
                  "queues (with running applications).").
            __(INSTANTANEOUS_FAIR_SHARE).__().
            span().$class("qlegend ui-corner-all").$style(Q_UNDER).
            __("Used").__().
            span().$class("qlegend ui-corner-all").$style(Q_OVER).
            __("Used (over fair share)").__().
            span().$class("qlegend ui-corner-all ui-state-default").
            __("Max Capacity").__().
            __().
          // 渲染根队列条目
          li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(join(width(used), ";left:0%;",
                  used > 1 ? Q_OVER : Q_UNDER)).__(".").__().
              span(".q", "root").__().
            span().$class("qstats").$style(left(Q_STATS_POS)).
            __(join(percent(used), " used")).__().
            __(QueueBlock.class).__();
      }
      // 闭合DOM标签，初始化JS树组件，渲染应用表格区块
      ul.__().__().
      script().$type("text/javascript").
          __("$('#cs').hide();").__().__().
          __(FairSchedulerAppsBlock.class);
    }
  }
  
  @Override
  /**
   * 在HTML头部添加页面所需CSS样式和JavaScript代码
   */
  protected void postHead(Page.HTML<__> html) {
    html.
      // 添加页面CSS样式
      style().$type("text/css").
        __("#cs { padding: 0.5em 0 1em 0; margin-bottom: 1em; position: relative }",
          "#cs ul { list-style: none }",
          "#cs a { font-weight: normal; margin: 2px; position: relative }",
          "#cs a span { font-weight: normal; font-size: 80% }",
          "#cs-wrapper .ui-widget-header { padding: 0.2em 0.5em }",
          ".qstats { font-weight: normal; font-size: 80%; position: absolute }",
          ".qlegend { font-weight: normal; padding: 0 1em; margin: 1em }",
          "table.info tr th {width: 50%}").__(). // 设置信息表格列宽居中
      // 引入jstree依赖JS
      script("/static/jt/jquery.jstree.js").
      // 初始化jstree组件并绑定点击筛选事件
      script().$type("text/javascript").
        __("$(function() {",
          "  $('#cs a span').addClass('ui-corner-all').css('position', 'absolute');",
          "  $('#cs').bind('loaded.jstree', function (e, data) {",
          "    var callback = { call:reopenQueryNodes }",
          "    data.inst.open_node('#pq', callback);",
          "   }).",
          "    jstree({",
          "    core: { animation: 188, html_titles: true },",
          "    plugins: ['themeroller', 'html_data', 'ui'],",
          "    themeroller: { item_open: 'ui-icon-minus',",
          "      item_clsd: 'ui-icon-plus', item_leaf: 'ui-icon-gear'",
          "    }",
          "  });",
          "  $('#cs').bind('select_node.jstree', function(e, data) {",
          "    var queues = $('.q', data.rslt.obj);",
          "    var q = '^' + queues.first().text();",
          "    q += queues.length == 1 ? '$' : '\\\\.';",
          "    $('#apps').dataTable().fnFilter(q, 4, true);",
          "  });",
          "  $('#cs').show();",
          "});").__().
        // 添加队列节点持久化JS
        __(SchedulerPageUtil.QueueBlockUtil.class);
  }
  
  @Override
  /**
   * 获取页面主内容区块类
   * @return 队列总区块类
   */
  protected Class<? extends SubView> content() {
    return QueuesBlock.class;
  }

  @Override
  protected String initAppsTable() {
    return WebPageUtils.appsTableInit(true, false);
  }

  /**
   * 格式化浮点数为百分比字符串，保留1位小数
   * @param f 浮点占比[0-1]
   * @return 格式化后的百分比字符串
   */
  static String percent(float f) {
    return StringUtils.formatPercent(f, 1);
  }

  /**
   * 生成宽度百分比CSS样式字符串
   * @param f 宽度占比[0-1]
   * @return CSS宽度属性字符串
   */
  static String width(float f) {
    return StringUtils.format("width:%.1f%%", f * 100);
  }

  /**
   * 生成左侧偏移百分比CSS样式字符串
   * @param f 左侧偏移占比[0-1]
   * @return CSS左侧偏移属性字符串
   */
  static String left(float f) {
    return StringUtils.format("left:%.1f%%", f * 100);
  }
}