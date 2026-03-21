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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import java.security.Principal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

import javax.servlet.http.HttpServletRequest;

/**
 * 公平调度器页面中展示公平调度器专属应用信息的HTML块
 */
public class FairSchedulerAppsBlock extends HtmlBlock {
  final ConcurrentMap<ApplicationId, RMApp> apps;
  final FairSchedulerInfo fsinfo;
  final Configuration conf;
  final ResourceManager rm;
  final boolean filterAppsByUser;

  /**
   * 构造函数，初始化应用列表并根据权限过滤可见应用
   * @param rm ResourceManager实例
   * @param ctx 视图上下文
   * @param conf Yarn配置
   */
  @Inject
  public FairSchedulerAppsBlock(ResourceManager rm, ViewContext ctx,
      Configuration conf) {
    super(ctx);
    this.conf = conf;
    this.rm = rm;

    // 读取是否按登录用户过滤应用的配置
    this.filterAppsByUser  = conf.getBoolean(
        YarnConfiguration.FILTER_ENTITY_LIST_BY_USER,
        YarnConfiguration.DEFAULT_DISPLAY_APPS_FOR_LOGGED_IN_USER);

    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    fsinfo = new FairSchedulerInfo(scheduler);
    apps = new ConcurrentHashMap<ApplicationId, RMApp>();
    // 遍历所有RM中应用，筛选符合条件的应用
    for (Map.Entry<ApplicationId, RMApp> entry : rm.getRMContext().getRMApps()
        .entrySet()) {
      // 只展示已经提交完成的应用，跳过新建/保存中状态
      if (!(RMAppState.NEW.equals(entry.getValue().getState())
          || RMAppState.NEW_SAVING.equals(entry.getValue().getState())
          || RMAppState.SUBMITTED.equals(entry.getValue().getState()))) {
        // 不需要过滤，或当前用户有权限查看该应用，加入列表
        if (!filterAppsByUser || hasAccess(entry.getValue(),
            ctx.requestContext().getRequest())) {
          apps.put(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  /**
   * 从HTTP请求中获取调用用户的UGI信息
   * @param hsr HTTP请求对象
   * @param usePrincipal 是否使用请求中的Principal获取用户名
   * @return 调用用户的UserGroupInformation，未获取到用户名则返回null
   */
  private UserGroupInformation getCallerUserGroupInformation(
      HttpServletRequest hsr, boolean usePrincipal) {
    String remoteUser = hsr.getRemoteUser();
    if (usePrincipal) {
      Principal princ = hsr.getUserPrincipal();
      remoteUser = princ == null ? null : princ.getName();
    }

    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }

    return callerUGI;
  }

  /**
   * 检查当前用户是否有权限查看指定应用
   * @param app 待检查应用
   * @param hsr HTTP请求对象
   * @return true表示有权限，false表示无权限
   */
  protected Boolean hasAccess(RMApp app, HttpServletRequest hsr) {
    // 获取调用用户UGI
    UserGroupInformation callerUGI = getCallerUserGroupInformation(hsr, true);
    List<String> forwardedAddresses = null;
    // 解析X-Forwarded-For请求头获取原始客户端IP
    String forwardedFor = hsr.getHeader(RMWSConsts.FORWARDED_FOR);
    if (forwardedFor != null) {
      forwardedAddresses = Arrays.asList(forwardedFor.split(","));
    }

    // 如果用户存在，且既无应用查看权限，也无队列管理权限，则拒绝访问
    if (callerUGI != null
        && !(this.rm.getApplicationACLsManager().checkAccess(callerUGI,
        ApplicationAccessType.VIEW_APP, app.getUser(),
        app.getApplicationId())
        || this.rm.getQueueACLsManager().checkAccess(callerUGI,
        QueueACL.ADMINISTER_QUEUE, app, hsr.getRemoteAddr(),
        forwardedAddresses))) {
      return false;
    }
    return true;
  }

  /**
   * 格式化应用信息数值，-1转换为N/A显示
   * @param value 待格式化数值
   * @return 格式化后的字符串
   */
  private static String printAppInfo(long value) {
    if (value == -1) {
      return "N/A";
    }
    return String.valueOf(value);
  }

  /**
   * 渲染公平调度器应用列表表格HTML
   * @param html HTML块输出对象
   */
  @Override public void render(Block html) {
    // 创建应用表格表头
    TBODY<TABLE<Hamlet>> tbody = html.
      table("#apps").
        thead().
          tr().
            th(".id", "ID").
            th(".user", "User").
            th(".name", "Name").
            th(".type", "Application Type").
            th(".queue", "Queue").
            th(".fairshare", "Fair Share").
            th(".starttime", "StartTime").
            th(".launchTime", "LaunchTime").
            th(".finishtime", "FinishTime").
            th(".state", "State").
            th(".finalstatus", "FinalStatus").
            th(".runningcontainer", "Running Containers").
            th(".allocatedCpu", "Allocated CPU VCores").
            th(".allocatedMemory", "Allocated Memory MB").
            th(".reservedCpu", "Reserved CPU VCores").
            th(".reservedMemory", "Reserved Memory MB").
            th(".progress", "Progress").
            th(".ui", "Tracking UI").__().__().
        tbody();
    // 解析请求中的应用状态过滤参数
    Collection<YarnApplicationState> reqAppStates = null;
    String reqStateString = $(APP_STATE);
    if (reqStateString != null && !reqStateString.isEmpty()) {
      String[] appStateStrings = reqStateString.split(",");
      reqAppStates = new HashSet<YarnApplicationState>(appStateStrings.length);
      for(String stateString : appStateStrings) {
        reqAppStates.add(YarnApplicationState.valueOf(stateString));
      }
    }
    // 构建前端表格所需的JSON数据
    StringBuilder appsTableData = new StringBuilder("[\n");
    // 遍历过滤后可见的所有应用
    for (RMApp app : apps.values()) {
      // 应用状态不匹配过滤条件，跳过
      if (reqAppStates != null && !reqAppStates.contains(app.createApplicationState())) {
        continue;
      }
      // 构建应用信息对象
      AppInfo appInfo = new AppInfo(rm, app, true, WebAppUtils.getHttpSchemePrefix(conf));
      // 格式化应用进度百分比
      String percent = StringUtils.format("%.1f", appInfo.getProgress());
      // 获取当前应用尝试ID
      ApplicationAttemptId attemptId = app.getCurrentAppAttempt().getAppAttemptId();
      // 从公平调度器信息中获取应用的公平份额
      long fairShare = fsinfo.getAppFairShare(attemptId);
      // 公平调度器中不存在该应用信息，跳过
      if (fairShare == FairSchedulerInfo.INVALID_FAIR_SHARE) {
        continue;
      }
      // 拼接应用信息JSON行
      appsTableData.append("[\"<a href='")
      .append(url("app", appInfo.getAppId())).append("'>")
      .append(appInfo.getAppId()).append("</a>\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        appInfo.getUser()))).append("\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        appInfo.getName()))).append("\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        appInfo.getApplicationType()))).append("\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        appInfo.getQueue()))).append("\",\"")
      .append(fairShare).append("\",\"")
      .append(appInfo.getStartTime()).append("\",\"")
      .append(appInfo.getLaunchTime()).append("\",\"")
      .append(appInfo.getFinishTime()).append("\",\"")
      .append(appInfo.getState()).append("\",\"")
      .append(appInfo.getFinalStatus()).append("\",\"")
      .append(printAppInfo(appInfo.getRunningContainers()))
      .append("\",\"")
      .append(printAppInfo(appInfo.getAllocatedVCores()))
      .append("\",\"")
      .append(printAppInfo(appInfo.getAllocatedMB()))
      .append("\",\"")
      .append(printAppInfo(appInfo.getReservedVCores()))
      .append("\",\"")
      .append(printAppInfo(appInfo.getReservedMB()))
      .append("\",\"")
      // 拼接进度条HTML
      .append("<br title='").append(percent)
      .append("'> <div class='").append(C_PROGRESSBAR).append("' title='")
      .append(join(percent, '%')).append("'> ").append("<div class='")
      .append(C_PROGRESSBAR_VALUE).append("' style='")
      .append(join("width:", percent, '%')).append("'> </div> </div>")
      .append("\",\"<a href='");

      // 处理跟踪URL，未就绪则显示#
      String trackingURL =
        !appInfo.isTrackingUrlReady()? "#" : appInfo.getTrackingUrlPretty();

      // 拼接跟踪UI链接
      appsTableData.append(trackingURL).append("'>")
      .append(appInfo.getTrackingUI()).append("</a>\"],\n");

    }
    // 移除最后一行多余的逗号
    if(appsTableData.charAt(appsTableData.length() - 2) == ',') {
      appsTableData.delete(appsTableData.length()-2, appsTableData.length()-1);
    }
    appsTableData.append("]");
    // 将应用数据输出为JavaScript变量供前端表格使用
    html.script().$type("text/javascript").
        __("var appsTableData=" + appsTableData).__();

    tbody.__().__();
  }
}