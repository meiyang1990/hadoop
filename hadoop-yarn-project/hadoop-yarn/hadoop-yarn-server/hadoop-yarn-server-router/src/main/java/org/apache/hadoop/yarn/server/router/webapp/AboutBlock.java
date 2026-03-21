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

package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterInfo;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * YARN Router Web UI 关于页面信息块，展示Router的基础信息和联邦状态。
 */
public class AboutBlock extends RouterBlock {

  private final Router router;

  @Inject
  AboutBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {
    // 获取YARN联邦是否启用状态
    boolean isEnabled = isYarnFederationEnabled();

    // 初始化用户帮助提示区域，未启用联邦时提示用户
    initUserHelpInformationDiv(html, isEnabled);

    // 渲染指标概览表格
    html.__(MetricsOverviewTable.class);

    // 初始化YARN Router基础信息面板
    initYarnRouterBasicInformation(isEnabled);

    // 渲染通用信息块
    html.__(InfoBlock.class);
  }

  /**
   * 初始化YARN Router基础信息，添加到信息面板展示。
   * @param isEnabled true=联邦已启用，false=联邦未启用
   */
  private void initYarnRouterBasicInformation(boolean isEnabled) {
    // 获取联邦状态存储门面实例
    FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance(router.getConfig());
    // 构建Router基础信息DAO
    RouterInfo routerInfo = new RouterInfo(router);
    // 格式化Router启动时间
    String lastStartTime =
        DateFormatUtils.format(routerInfo.getStartedOn(), DATE_PATTERN);
    try {
      // 创建Overview信息块，添加各项Router信息
      info("Yarn Router Overview").
          __("Federation Enabled:", String.valueOf(isEnabled)).
          __("Router ID:", routerInfo.getClusterId()).
          __("Router state:", routerInfo.getState()).
          __("Router SubCluster Count:", facade.getSubClusters(true).size()).
          __("Router RMStateStore:", routerInfo.getRouterStateStore()).
          __("Router started on:", lastStartTime).
          __("Router version:", routerInfo.getRouterBuildVersion() +
             " on " + routerInfo.getRouterVersionBuiltOn()).
          __("Hadoop version:", routerInfo.getHadoopBuildVersion() +
             " on " + routerInfo.getHadoopVersionBuiltOn());
    } catch (YarnException e) {
      // 初始化失败记录日志
      LOG.error("initYarnRouterBasicInformation error.", e);
    }
  }
}