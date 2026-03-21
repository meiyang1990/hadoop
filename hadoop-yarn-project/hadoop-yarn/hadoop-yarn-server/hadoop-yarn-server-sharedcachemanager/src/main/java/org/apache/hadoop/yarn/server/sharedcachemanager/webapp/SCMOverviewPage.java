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

package org.apache.hadoop.yarn.server.sharedcachemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.sharedcachemanager.SharedCacheManager;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.ClientSCMMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.SharedCacheUploaderMetrics;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import com.google.inject.Inject;

/**
 * 共享缓存管理器(SCM) Web UI 概览页面渲染类
 * 负责生成SCM监控页面的整体布局结构
 */
@Private
@Unstable
public class SCMOverviewPage extends TwoColumnLayout {

  /**
   * 在HTML head部分初始化JQuery UI手风琴导航组件
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 设置导航手风琴组件ID
    set(ACCORDION_ID, "nav");
    // 初始化手风琴配置：禁用自动高度，默认展开第一个菜单项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override protected Class<? extends SubView> content() {
    // 主内容区使用SCM概览信息块
    return SCMOverviewBlock.class;
  }

  @Override
  protected Class<? extends SubView> nav() {
    // 侧边导航区使用SCM导航块
    return SCMOverviewNavBlock.class;
  }

  /**
   * 侧边导航栏块，渲染工具链接菜单
   */
  static private class SCMOverviewNavBlock extends HtmlBlock {
    @Override
    protected void render(Block html) {
      // 渲染工具菜单，包含配置、线程转储、日志、指标四个链接
      html.div("#nav").h3("Tools").ul().li().a("/conf", "Configuration").__()
          .li().a("/stacks", "Thread dump").__().li().a("/logs", "Logs").__()
          .li().a("/metrics", "Metrics").__().__().__();
    }
  }

  /**
   * 主内容块，渲染SCM核心概览信息
   */
  static private class SCMOverviewBlock extends HtmlBlock {
    // SCM服务实例引用，用于获取运行时信息
    final SharedCacheManager scm;

    @Inject
    SCMOverviewBlock(SharedCacheManager scm, ViewContext ctx) {
      super(ctx);
      this.scm = scm;
    }

    @Override
    protected void render(Block html) {
      // 聚合各模块指标数据
      SCMMetricsInfo metricsInfo = new SCMMetricsInfo(
          CleanerMetrics.getInstance(), ClientSCMMetrics.getInstance(),
              SharedCacheUploaderMetrics.getInstance());
      // 构建概览信息表格
      info("Shared Cache Manager overview").
          __("Started on:", Times.format(scm.getStartTime())).
          __("Cache hits: ", metricsInfo.getCacheHits()).
          __("Cache misses: ", metricsInfo.getCacheMisses()).
          __("Cache releases: ", metricsInfo.getCacheReleases()).
          __("Accepted uploads: ", metricsInfo.getAcceptedUploads()).
          __("Rejected uploads: ", metricsInfo.getRejectUploads()).
          __("Deleted files by the cleaner: ", metricsInfo.getTotalDeletedFiles()).
          __("Processed files by the cleaner: ", metricsInfo.getTotalProcessedFiles());
      // 渲染信息块到页面
      html.__(InfoBlock.class);
    }
  }
}