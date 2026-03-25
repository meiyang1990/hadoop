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

import static org.apache.commons.text.StringEscapeUtils.escapeHtml4;
import static org.apache.commons.text.StringEscapeUtils.escapeEcmaScript;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_SC;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.inject.Inject;

import javax.ws.rs.client.Client;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Router Web UI 应用列表展示块，负责在联邦集群页面展示聚合后的应用信息。
 */
public class AppsBlock extends RouterBlock {

  private final Router router;
  private final Configuration conf;

  @Inject
  AppsBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
    this.conf = this.router.getConfig();
  }

  @Override
  protected void render(Block html) {
    // 检查YARN联邦模式是否启用
    boolean isEnabled = isYarnFederationEnabled();

    // 获取请求参数中的子集群ID
    String subClusterName = $(APP_SC);
    // 获取请求参数中的应用状态过滤条件
    String reqState = $(APP_STATE);

    AppsInfo appsInfo = null;
    // 如果指定了子集群，查询该子集群的应用列表
    if (subClusterName != null && !subClusterName.isEmpty()) {
      initSubClusterMetricsOverviewTable(html, subClusterName);
      appsInfo = getSubClusterAppsInfo(subClusterName, reqState);
    } else {
      // 未指定子集群，展示全局聚合指标和全集群应用列表
      html.__(MetricsOverviewTable.class);
      appsInfo = getYarnFederationAppsInfo(isEnabled);
    }

    // 初始化应用列表表格并渲染
    initYarnFederationAppsOfCluster(appsInfo, html);
  }

  /**
   * 对字符串进行HTML和JS转义，避免XSS攻击。
   * @param str 原始字符串
   * @return 转义后的字符串
   */
  private static String escape(String str) {
    return escapeEcmaScript(escapeHtml4(str));
  }

  /**
   * 获取YARN联邦集群全局聚合后的应用列表。
   * @param isEnabled 联邦模式是否启用
   * @return 聚合后的应用信息
   */
  private AppsInfo getYarnFederationAppsInfo(boolean isEnabled) {
    String webAddress = null;
    if (isEnabled) {
      // 联邦模式启用，从Router获取全局应用列表
      webAddress = WebAppUtils.getRouterWebAppURLWithScheme(this.conf);
    } else {
      // 联邦模式未启用，直接从本地RM获取应用列表
      webAddress = WebAppUtils.getRMWebAppURLWithScheme(this.conf);
    }
    return getSubClusterAppsInfoByWebAddress(webAddress, StringUtils.EMPTY);
  }

  /**
   * 根据子集群ID从对应子集群RM获取应用列表。
   * @param subCluster 子集群ID
   * @param states 应用状态过滤条件
   * @return 子集群应用信息
   */
  private AppsInfo getSubClusterAppsInfo(String subCluster, String states) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subCluster);
      // 获取联邦状态存储门面，查询子集群信息
      FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance(this.conf);
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);

      if (subClusterInfo != null) {
        // 获取子集群RM的Web服务地址
        String webAddress = subClusterInfo.getRMWebServiceAddress();
        String herfWebAppAddress;
        if (webAddress != null && !webAddress.isEmpty()) {
          // 拼接完整的Web服务URL
          herfWebAppAddress = WebAppUtils.getHttpSchemePrefix(conf) + webAddress;
          // 通过REST接口获取子集群应用列表
          return getSubClusterAppsInfoByWebAddress(herfWebAppAddress, states);
        }
      }
    } catch (Exception e) {
      LOG.error("get AppsInfo From SubCluster = {} error.", subCluster, e);
    }
    return null;
  }

  /**
   * 通过指定Web地址调用REST接口获取应用列表。
   * @param webAddress 目标Web服务地址
   * @param states 应用状态过滤条件
   * @return 应用信息列表
   */
  private AppsInfo getSubClusterAppsInfoByWebAddress(String webAddress, String states) {
    // 创建Jersey客户端
    Client client = RouterWebServiceUtil.createJerseyClient(conf);
    // 构造请求参数
    Map<String, String[]> queryParams = new HashMap<>();
    if (StringUtils.isNotBlank(states)) {
      queryParams.put("states", new String[]{states});
    }
    // 转发请求到目标服务获取应用列表
    AppsInfo apps = RouterWebServiceUtil
        .genericForward(webAddress, null, AppsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, null, queryParams, conf,
        client);
    // 关闭客户端释放资源
    client.close();
    return apps;
  }

  /**
   * 初始化应用列表表格的HTML结构，并将应用数据注入页面供前端渲染。
   * @param appsInfo 应用信息列表
   * @param html HTML块对象
   */
  private void initYarnFederationAppsOfCluster(AppsInfo appsInfo, Block html) {
    // 创建应用表格表头
    TBODY<TABLE<Hamlet>> tbody = html.table("#apps").thead()
        .tr()
        .th(".id", "ID")
        .th(".user", "User")
        .th(".name", "Name")
        .th(".type", "Application Type")
        .th(".queue", "Queue")
        .th(".priority", "Application Priority")
        .th(".starttime", "StartTime")
        .th(".finishtime", "FinishTime")
        .th(".state", "State")
        .th(".finalstatus", "FinalStatus")
        .th(".progress", "Progress")
        .th(".ui", "Tracking UI")
        .__().__().tbody();

    // 构建前端表格需要的JSON数据
    StringBuilder appsTableData = new StringBuilder("[\n");

    if (appsInfo != null && CollectionUtils.isNotEmpty(appsInfo.getApps())) {
      // 遍历转换每个应用信息为JSON格式字符串
      List<String> appInfoList =
          appsInfo.getApps().stream().map(this::parseAppInfoData).collect(Collectors.toList());

      if (CollectionUtils.isNotEmpty(appInfoList)) {
        String formattedAppInfo = StringUtils.join(appInfoList, ",");
        appsTableData.append(formattedAppInfo);
      }
    }

    appsTableData.append("]");
    // 将应用数据注入为页面全局JavaScript变量
    html.script().$type("text/javascript")
        .__("var appsTableData=" + appsTableData).__();

    tbody.__().__();
  }

  /**
   * 将单个应用信息解析为前端表格需要的JSON行格式。
   * @param app 应用信息对象
   * @return JSON格式的应用行字符串
   */
  private String parseAppInfoData(AppInfo app) {
    StringBuilder appsDataBuilder = new StringBuilder();
    try {
      // 格式化应用进度百分比
      String percent = String.format("%.1f", app.getProgress() * 100.0F);
      // 获取追踪页面URL
      String trackingURL = app.getTrackingUrl() == null ? "#" : app.getTrackingUrl();

      // 拼接应用信息，构造JSON数组行
      appsDataBuilder.append("[\"")
          .append("<a href='").append(trackingURL).append("'>")
          .append(app.getAppId()).append("</a>\",\"")
          .append(escape(app.getUser())).append("\",\"")
          .append(escape(app.getName())).append("\",\"")
          .append(escape(app.getApplicationType())).append("\",\"")
          .append(escape(app.getQueue())).append("\",\"")
          .append(app.getPriority()).append("\",\"")
          .append(app.getStartTime()).append("\",\"")
          .append(app.getFinishTime()).append("\",\"")
          .append(app.getState()).append("\",\"")
          .append(app.getFinalStatus()).append("\",\"")
          // 进度条HTML
          .append("<br title='").append(percent).append("'> <div class='")
          .append(C_PROGRESSBAR).append("' title='")
          .append(join(percent, '%')).append("'> ").append("<div class='")
          .append(C_PROGRESSBAR_VALUE).append("' style='")
          .append(join("width:", percent, '%')).append("'> </div> </div>")
          // 追踪链接
          .append("\",\"<a href='").append(trackingURL).append("'>")
          .append("History").append("</a>");
      appsDataBuilder.append("\"]\n");

    } catch (Exception e) {
      LOG.warn("Cannot add application {}: {}", app.getAppId(), e.getMessage());
    }
    return appsDataBuilder.toString();
  }
}