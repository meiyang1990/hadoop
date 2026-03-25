// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.webproxy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * 默认的应用报告获取实现，从ResourceManager或应用历史服务器获取YARN应用报告，
 * 为Web代理提供应用信息查询能力。
 */
public class DefaultAppReportFetcher extends AppReportFetcher {

  private final ApplicationClientProtocol applicationsManager;
  private String rmAppPageUrlBase;

  /**
   * 构造函数，自动创建到ResourceManager的代理连接，用于获取应用报告。
   *
   * @param conf YARN配置，包含ResourceManager地址信息
   */
  public DefaultAppReportFetcher(Configuration conf) {
    super(conf);
    // 拼接ResourceManager应用页面的基础URL
    this.rmAppPageUrlBase =
        StringHelper.pjoin(WebAppUtils.getResolvedRMWebAppURLWithScheme(conf), "cluster", "app");
    try {
      // 创建ResourceManager代理客户端
      this.applicationsManager = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * 构造函数，使用已有的ApplicationClientProtocol连接，适用于代理运行在RM内部的场景。
   *
   * @param conf                YARN配置
   * @param applicationsManager 已初始化的RM客户端协议对象
   */
  public DefaultAppReportFetcher(Configuration conf,
      ApplicationClientProtocol applicationsManager) {
    super(conf);
    // 拼接ResourceManager应用页面的基础URL
    this.rmAppPageUrlBase =
        StringHelper.pjoin(WebAppUtils.getResolvedRMWebAppURLWithScheme(conf), "cluster", "app");
    this.applicationsManager = applicationsManager;
  }

  /**
   * 获取指定应用的报告，优先从RM查询，找不到则回退到应用历史服务器。
   *
   * @param appId 目标应用ID
   * @return 应用报告封装对象
   * @throws YarnException YARN服务端异常
   * @throws IOException   网络连接异常
   */
  @Override
  public FetchedAppReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    return super.getApplicationReport(applicationsManager, appId);
  }

  /**
   * 获取ResourceManager应用页面的基础URL。
   *
   * @param appId 目标应用ID
   * @return RM应用页面基础URL字符串
   * @throws YarnException YARN异常
   * @throws IOException   IO异常
   */
  public String getRmAppPageUrlBase(ApplicationId appId) throws YarnException, IOException {
    return this.rmAppPageUrlBase;
  }

  /**
   * 停止服务，关闭RPC代理连接释放资源。
   */
  public void stop() {
    super.stop();
    if (this.applicationsManager != null) {
      RPC.stopProxy(this.applicationsManager);
    }
  }
}