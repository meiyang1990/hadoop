// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this use this file except in compliance
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

package org.apache.hadoop.yarn.server.router.webapp;

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.List;
/**
 * Router Web UI 页面导航块，负责生成页面顶部导航菜单
 */
public class NavBlock extends RouterBlock {

  private Router router;

  @Inject
  public NavBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  public void render(Block html) {
    // 根据YARN联邦是否开启，设置导航链接文本
    String federationText = isYarnFederationEnabled() ? "Federation" : "LocalCluster";
    // 构建主导航栏容器，添加Cluster分组和基础菜单项
    Hamlet.UL<Hamlet.DIV<Hamlet>> mainList = html.div("#nav").
        h3("Cluster").
        ul().
        li().a(url(""), "About").__().
        li().a(url("federation"), federationText).__();
    // 获取所有活跃子集群ID列表
    List<String> subClusterIds = getActiveSubClusterIds();

    // 初始化节点信息下拉菜单
    initNodesMenu(mainList, subClusterIds);

    // 初始化节点标签信息下拉菜单
    initNodeLabelsMenu(mainList, subClusterIds);

    // 初始化应用信息下拉菜单
    initApplicationsMenu(mainList, subClusterIds);

    // 初始化工具菜单分组
    Hamlet.DIV<Hamlet> sectionBefore = mainList.__();
    Configuration conf = new Configuration();
    // 附加通用工具菜单项
    Hamlet.UL<Hamlet.DIV<Hamlet>> tools = WebPageUtils.appendToolSection(sectionBefore, conf);

    if (tools == null) {
      return;
    }
    // 关闭导航菜单DOM标签
    tools.__().__();
  }
}