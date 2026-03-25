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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import javax.servlet.Filter;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

/**
 * YARN Router的Web应用，提供Federation联邦场景下的Web UI和REST API服务。
 * 继承自Hadoop通用WebApp基类，实现YARN Web参数常量接口。
 */
public class RouterWebApp extends WebApp implements YarnWebParams {
  private Router router;

  /**
   * 构造函数，关联Router服务实例。
   * @param router Router服务实例
   */
  public RouterWebApp(Router router) {
    this.router = router;
  }

  @Override
  /**
   * 初始化Web应用路由配置，绑定URL路径到对应控制器方法。
   */
  public void setup() {
    // 如果Router实例存在，将其绑定到Guice容器
    if (router != null) {
      bind(Router.class).toInstance(router);
    }
    // 绑定根路径到RouterController首页
    route("/", RouterController.class);
    // 绑定集群信息路径到about方法
    route("/cluster", RouterController.class, "about");
    // 绑定关于页面到about方法
    route("/about", RouterController.class, "about");
    // 绑定应用列表路径，支持状态和用户过滤参数到apps方法
    route(pajoin("/apps", APP_SC, APP_STATE), RouterController.class, "apps");
    // 绑定节点列表路径，支持状态过滤到nodes方法
    route(pajoin("/nodes", NODE_SC), RouterController.class, "nodes");
    // 绑定联邦信息页面到federation方法
    route("/federation", RouterController.class, "federation");
    // 绑定节点标签页面到nodeLabels方法
    route(pajoin("/nodelabels", NODE_SC), RouterController.class, "nodeLabels");
  }

  /**
   * 构建Jersey REST资源配置，注册REST服务和组件。
   * @return 配置完成的Jersey ResourceConfig实例
   */
  public ResourceConfig resourceConfig() {
    ResourceConfig config = new ResourceConfig();
    // 扫描webapp包下的REST资源
    config.packages("org.apache.hadoop.yarn.server.router.webapp");
    // 注册依赖注入Binder
    config.register(new JerseyBinder());
    // 注册Router REST Web服务
    config.register(RouterWebServices.class);
    // 注册JSON序列化相关组件
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  /**
   * Jersey依赖注入Binder，将Router实例和配置注入到Jersey容器。
   */
  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      // 绑定Router实例，命名为"router"
      bind(router).to(Router.class).named("router");
      // 绑定Router配置，命名为"conf"
      bind(router.getConfig()).to(Configuration.class).named("conf");
    }
  }

  @Override
  /**
   * 获取Web应用过滤器类，当前Router不需要额外过滤器，返回null。
   * @return 过滤器类，此处为null
   */
  protected Class<? extends Filter> getWebAppFilterClass() {
    return null;
  }
}