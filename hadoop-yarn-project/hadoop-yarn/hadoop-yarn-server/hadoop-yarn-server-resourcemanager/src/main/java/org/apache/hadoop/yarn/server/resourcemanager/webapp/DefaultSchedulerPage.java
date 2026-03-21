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

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.webapp.AppsBlock;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * FIFO调度器Web页面渲染器，负责生成YARN ResourceManager中FIFO调度器的监控页面内容
 */
class DefaultSchedulerPage extends RmView {
  // 队列条默认CSS样式类
  static final String _Q = ".ui-state-default.ui-corner-all";
  // 容量条宽度占比
  static final float WIDTH_F = 0.8f;
  // 容量条结束位置样式
  static final String Q_END = "left:101%";
  // 容量超过配置时的高亮样式（橙色）
  static final String OVER = "font-size:1px;background:#FFA333";
  // 容量低于配置时的高亮样式（绿色）
  static final String UNDER = "font-size:1px;background:#5BD75B";
  // 浮点比较容差
  static final float EPSILON = 1e-8f;

  /**
   * 队列状态信息块，渲染FIFO队列基本状态信息
   */
  static class QueueInfoBlock extends HtmlBlock {
    final FifoSchedulerInfo sinfo;

    @Inject
    QueueInfoBlock(ViewContext ctx, ResourceManager rm) {
      super(ctx);
      // 从ResourceManager获取FIFO调度器信息
      sinfo = new FifoSchedulerInfo(rm);
    }

    @Override public void render(Block html) {
      // 构建队列状态信息面板
      info("\'" + sinfo.getQueueName() + "\' Queue Status").
          __("Queue State:" , sinfo.getState()).
          __("Minimum Queue Memory Capacity:" , Long.toString(sinfo.getMinQueueMemoryCapacity())).
          __("Maximum Queue Memory Capacity:" , Long.toString(sinfo.getMaxQueueMemoryCapacity())).
          __("Number of Nodes:" , Integer.toString(sinfo.getNumNodes())).
          __("Used Node Capacity:" , Integer.toString(sinfo.getUsedNodeCapacity())).
          __("Available Node Capacity:" , Integer.toString(sinfo.getAvailNodeCapacity())).
          __("Total Node Capacity:" , Integer.toString(sinfo.getTotalNodeCapacity())).
          __("Number of Node Containers:" , Integer.toString(sinfo.getNumContainers()));

      // 渲染信息块
      html.__(InfoBlock.class);
    }
  }

  /**
   * 队列列表块，渲染FIFO调度器队列总览和应用列表
   */
  static class QueuesBlock extends HtmlBlock {
    final FifoSchedulerInfo sinfo;
    final FifoScheduler fs;

    @Inject QueuesBlock(ResourceManager rm) {
      sinfo = new FifoSchedulerInfo(rm);
      // 从ResourceManager获取FIFO调度器实例
      fs = (FifoScheduler) rm.getResourceScheduler();
    }

    @Override
    public void render(Block html) {
      // 渲染指标概览表
      html.__(MetricsOverviewTable.class);
      // 构建队列容器DOM结构
      UL<DIV<DIV<Hamlet>>> ul = html.
        div("#cs-wrapper.ui-widget").
          div(".ui-widget-header.ui-corner-top").
          __("FifoScheduler Queue").__().
          div("#cs.ui-widget-content.ui-corner-bottom").
            ul();

      if (fs == null) {
        // 调度器未初始化，渲染默认空队列
        ul.
          li().
            a(_Q).$style(width(WIDTH_F)).
              span().$style(Q_END).__("100% ").__().
              span(".q", "default").__().__();
      } else {
        // 计算容量使用率，生成容量可视化条
        float used = sinfo.getUsedCapacity();
        float set = sinfo.getCapacity();
        float delta = Math.abs(set - used) + 0.001f;
        ul.
          li().
            a(_Q).$style(width(WIDTH_F)).
              $title(join("used:", percent(used))).
              span().$style(Q_END).__("100%").__().
              span().$style(join(width(delta), ';', used > set ? OVER : UNDER,
                ';', used > set ? left(set) : left(used))).__(".").__().
              span(".q", sinfo.getQueueName()).__().
            __(QueueInfoBlock.class).__();
      }

      // 闭合DOM标签，加载JS并渲染应用列表块
      ul.__().__().
      script().$type("text/javascript").
          __("$('#cs').hide();").__().__().
          __(AppsBlock.class);
    }
  }


  /**
   * 在HTML头部注入页面样式和交互JavaScript
   */
  @Override protected void postHead(Page.HTML<__> html) {
    html.
      style().$type("text/css").
        __("#cs { padding: 0.5em 0 1em 0; margin-bottom: 1em; position: relative }",
          "#cs ul { list-style: none }",
          "#cs a { font-weight: normal; margin: 2px; position: relative }",
          "#cs a span { font-weight: normal; font-size: 80% }",
          "#cs-wrapper .ui-widget-header { padding: 0.2em 0.5em }",
          "table.info tr th {width: 50%}").__(). // to center info table
      script("/static/jt/jquery.jstree.js").
      script().$type("text/javascript").
        __("$(function() {",
          "  $('#cs a span').addClass('ui-corner-all').css('position', 'absolute');",
          "  $('#cs').bind('loaded.jstree', function (e, data) {",
          "    data.inst.open_all(); }).",
          "    jstree({",
          "    core: { animation: 188, html_titles: true },",
          "    plugins: ['themeroller', 'html_data', 'ui'],",
          "    themeroller: { item_open: 'ui-icon-minus',",
          "      item_clsd: 'ui-icon-plus', item_leaf: 'ui-icon-gear'",
          "    }",
          "  });",
          // 绑定队列点击事件，点击后过滤对应队列的应用
          "  $('#cs').bind('select_node.jstree', function(e, data) {",
          "    var q = $('.q', data.rslt.obj).first().text();",
            "    if (q == 'root') q = '';",
          "    $('#apps').dataTable().fnFilter(q, 4);",
          "  });",
          "  $('#cs').show();",
          "});").__();
  }

  @Override protected Class<? extends SubView> content() {
    // 返回主内容块为队列块
    return QueuesBlock.class;
  }

  /**
   * 将浮点比例格式化为百分比字符串
   * @param f 浮点比例（0~1）
   * @return 格式化后的百分比字符串
   */
  static String percent(float f) {
    return StringUtils.formatPercent(f, 1);
  }

  /**
   * 生成CSS宽度百分比样式字符串
   * @param f 宽度比例（0~1）
   * @return CSS宽度样式字符串
   */
  static String width(float f) {
    return StringUtils.format("width:%.1f%%", f * 100);
  }

  /**
   * 生成CSS左偏移百分比样式字符串
   * @param f 偏移比例（0~1）
   * @return CSS左偏移样式字符串
   */
  static String left(float f) {
    return StringUtils.format("left:%.1f%%", f * 100);
  }
}