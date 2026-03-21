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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.sharedcachemanager.SharedCacheManager;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 共享缓存管理器(SCM)的Web服务端，提供Web界面查看共享缓存指标信息
 * 目前仅实现基础指标展示，TODO: 待添加Web UI安全机制(YARN-2774)
 */
@Private
@Unstable
public class SCMWebServer extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMWebServer.class);

  private final SharedCacheManager scm;
  private WebApp webApp;
  private String bindAddress;

  /**
   * 构造SCM Web服务实例
   * @param scm 所属共享缓存管理器实例
   */
  public SCMWebServer(SharedCacheManager scm) {
    super(SCMWebServer.class.getName());
    this.scm = scm;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 获取Web服务绑定地址
    this.bindAddress = getBindAddress(conf);
    super.serviceInit(conf);
  }

  /**
   * 从配置中读取Web服务绑定地址，使用默认值兜底
   * @param conf 配置对象
   * @return 绑定地址字符串
   */
  private String getBindAddress(Configuration conf) {
    return conf.get(YarnConfiguration.SCM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_WEBAPP_ADDRESS);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 创建SCM Web应用实例
    SCMWebApp scmWebApp = new SCMWebApp(scm);
    // 启动Web服务，绑定到配置的地址
    this.webApp = WebApps.$for("sharedcache").at(bindAddress).start(scmWebApp);
    LOG.info("Instantiated " + SCMWebApp.class.getName() + " at " + bindAddress);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 停止Web应用
    if (this.webApp != null) {
      this.webApp.stop();
    }
    super.serviceStop();
  }

  /**
   * SCM Web应用内部类，负责路由和依赖绑定
   */
  private class SCMWebApp extends WebApp {
    private final SharedCacheManager scm;

    public SCMWebApp(SharedCacheManager scm) {
      this.scm = scm;
    }

    @Override
    public void setup() {
      // 绑定共享缓存管理器实例到依赖注入容器
      if (scm != null) {
        bind(SharedCacheManager.class).toInstance(scm);
      }
      // 配置根路径路由到总览页面
      route("/", SCMController.class, "overview");
    }
  }
}