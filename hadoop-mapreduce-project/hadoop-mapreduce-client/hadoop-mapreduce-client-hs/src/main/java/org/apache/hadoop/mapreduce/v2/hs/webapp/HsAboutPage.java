// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain obtain a copy of the License at
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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.HistoryInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

/**
 *  MapReduce历史服务器关于页面，展示历史服务器版本信息与启动信息
 */
public class HsAboutPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 页面渲染前初始化页面头部，配置导航菜单与页面标题
   * @param html HTML页面对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    // 覆盖公共preHead中的导航配置，初始化导航折叠面板
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
    setTitle("About History Server");
  }

  /**
   * 获取页面内容子视图，组装历史服务器信息
   * @return 内容子视图类
   */
  @Override protected Class<? extends SubView> content() {
    HistoryInfo info = new HistoryInfo();
    // 组装页面展示信息
    info("History Server").
        __("BuildVersion", info.getHadoopBuildVersion()
        + " on " + info.getHadoopVersionBuiltOn()).
        __("History Server started on", Times.format(info.getStartedOn()));
    return InfoBlock.class;
  }
}