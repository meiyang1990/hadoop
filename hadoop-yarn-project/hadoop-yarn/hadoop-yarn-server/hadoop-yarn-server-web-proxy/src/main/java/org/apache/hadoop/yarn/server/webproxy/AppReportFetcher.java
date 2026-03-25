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

package org.apache.hadoop.yarn.server.webproxy;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.AHSProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * 应用报告获取抽象基类，封装从ResourceManager或应用历史服务器获取应用报告的通用逻辑
 * 为Web代理提供统一的应用信息获取入口
 */
public abstract class AppReportFetcher {

  /** 应用报告来源枚举：来自活跃RM还是已完成的AHS */
  protected enum AppReportSource {RM, AHS}

  private final Configuration conf;
  private ApplicationHistoryProtocol historyManager;
  private String ahsAppPageUrlBase;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private boolean isAHSEnabled;

  /**
   * 构造AppReportFetcher，初始化与RM/AHS的连接
   *
   * @param conf YARN配置，用于获取服务地址和配置信息
   */
  public AppReportFetcher(Configuration conf) {
    this.conf = conf;
    // 检查是否启用应用历史服务
    if (conf.getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
      this.isAHSEnabled = true;
      // 构建AHS应用页面基础URL
      String scheme = WebAppUtils.getHttpSchemePrefix(conf);
      String historyUrl = WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
      this.ahsAppPageUrlBase = StringHelper.pjoin(scheme + historyUrl, "applicationhistory", "app");
    }
    try {
      // 如果启用AHS，创建AHS代理客户端
      if (this.isAHSEnabled) {
        this.historyManager = getAHSProxy(conf);
      } else {
        this.historyManager = null;
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * 创建应用历史服务器RPC代理客户端
   * @param configuration YARN配置
   * @return AHS协议代理对象
   * @throws IOException 创建代理失败时抛出异常
   */
  protected ApplicationHistoryProtocol getAHSProxy(Configuration configuration)
      throws IOException {
    // 从配置中获取AHS服务地址
    InetSocketAddress addr = configuration.getSocketAddr(YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_PORT);
    // 通过AHSProxy工厂创建代理
    return AHSProxy.createAHSProxy(configuration, ApplicationHistoryProtocol.class, addr);
  }

  /**
   * 获取指定应用ID的应用报告，子类实现具体获取逻辑
   * 优先从RM获取，找不到则回退到AHS
   * @param appId 目标应用ID
   * @return 包含来源信息的应用报告
   * @throws YarnException YARN服务错误
   * @throws IOException 网络IO错误
   */
  public abstract FetchedAppReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException;

  /**
   * 通用获取应用报告逻辑：优先从RM获取，找不到则回退到AHS
   *
   * @param applicationsManager RM客户端协议对象
   * @param appId 目标应用ID
   * @return 包含来源信息的应用报告
   * @throws YarnException YARN服务错误
   * @throws IOException 网络IO错误
   */
  protected FetchedAppReport getApplicationReport(ApplicationClientProtocol applicationsManager,
      ApplicationId appId) throws YarnException, IOException {
    // 创建获取应用报告请求对象
    GetApplicationReportRequest request =
        this.recordFactory.newRecordInstance(GetApplicationReportRequest.class);
    // 设置目标应用ID
    request.setApplicationId(appId);

    ApplicationReport appReport;
    FetchedAppReport fetchedAppReport;
    try {
      // 从RM获取应用报告，标记来源为RM
      appReport = applicationsManager.getApplicationReport(request).getApplicationReport();
      fetchedAppReport = new FetchedAppReport(appReport, AppReportSource.RM);
    } catch (ApplicationNotFoundException e) {
      // AHS未启用时直接抛出异常
      if (!isAHSEnabled) {
        // Just throw it as usual if historyService is not enabled.
        throw e;
      }
      // RM找不到应用，从AHS获取已完成应用的报告，标记来源为AHS
      appReport = historyManager.getApplicationReport(request).getApplicationReport();
      fetchedAppReport = new FetchedAppReport(appReport, AppReportSource.AHS);
    }
    return fetchedAppReport;
  }

  /**
   * 获取RM端应用页面的基础URL，子类实现具体逻辑
   * @param appId 目标应用ID
   * @return RM应用页面基础URL
   * @throws IOException 网络IO错误
   * @throws YarnException YARN服务错误
   */
  public abstract String getRmAppPageUrlBase(ApplicationId appId) throws IOException, YarnException;

  /**
   * 获取AHS端应用页面的基础URL
   * @return AHS应用页面基础URL
   */
  public String getAhsAppPageUrlBase() {
    return this.ahsAppPageUrlBase;
  }

  protected Configuration getConf() {
    return this.conf;
  }

  /**
   * 停止服务，关闭AHS代理连接
   */
  public void stop() {
    if (this.historyManager != null) {
      RPC.stopProxy(this.historyManager);
    }
  }

  @VisibleForTesting
  public void setHistoryManager(ApplicationHistoryProtocol historyManager) {
    this.historyManager = historyManager;
  }

  /**
   * 封装应用报告及其来源的容器类，供Web代理根据来源做不同处理
   */
  protected static class FetchedAppReport {
    private ApplicationReport appReport;
    private AppReportSource appReportSource;

    public FetchedAppReport(ApplicationReport appReport, AppReportSource appReportSource) {
      this.appReport = appReport;
      this.appReportSource = appReportSource;
    }

    public AppReportSource getAppReportSource() {
      return this.appReportSource;
    }

    public ApplicationReport getApplicationReport() {
      return this.appReport;
    }
  }
}