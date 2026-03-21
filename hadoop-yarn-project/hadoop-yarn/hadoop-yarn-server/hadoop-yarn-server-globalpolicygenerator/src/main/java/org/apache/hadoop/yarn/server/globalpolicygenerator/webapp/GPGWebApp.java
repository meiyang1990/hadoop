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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * 全局策略生成器(GPG)的Web应用入口，负责注册路由、初始化Jersey资源配置和依赖注入绑定。
 * 提供GPG Web UI和REST API服务的基础配置能力。
 */
public class GPGWebApp extends WebApp {
  private GlobalPolicyGenerator gpg;

  /**
   * 构造GPG Web应用实例，持有全局策略生成器核心服务引用。
   * @param gpg 全局策略生成器核心服务实例
   */
  public GPGWebApp(GlobalPolicyGenerator gpg) {
    this.gpg = gpg;
  }

  @Override
  public void setup() {
    // 绑定当前WebApp实例到依赖注入容器
    bind(GPGWebApp.class).toInstance(this);
    if (gpg != null) {
      // 绑定全局策略生成器实例到依赖注入容器
      bind(GlobalPolicyGenerator.class).toInstance(gpg);
    }
    // 注册根路径路由到概览页面
    route("/", GPGController.class, "overview");
    // 注册策略查看路径路由到策略列表页面
    route("/policies", GPGController.class, "policies");
  }

  /**
   * 构建Jersey REST资源配置，注册所有Web相关组件。
   * @return 配置完成的Jersey ResourceConfig实例
   */
  public ResourceConfig resourceConfig() {
    ResourceConfig config = new ResourceConfig();
    // 扫描当前包下的Jersey资源
    config.packages("org.apache.hadoop.yarn.server.globalpolicygenerator.webapp");
    // 注册自定义依赖注入Binder
    config.register(new JerseyBinder());
    // 注册REST Web服务端点
    config.register(GPGWebServices.class);
    // 注册全局异常处理器
    config.register(GenericExceptionHandler.class);
    // 注册JSON序列化功能和JAXB上下文解析器
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  /**
   * Jersey依赖注入配置Binder，负责将核心服务实例注册到注入容器。
   */
  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      // 绑定GPG实例并命名为"gpg"
      bind(gpg).to(GlobalPolicyGenerator.class).named("gpg");
      // 绑定GPG配置实例并命名为"conf"
      bind(gpg.getConfig()).to(Configuration.class).named("conf");
    }
  }
}