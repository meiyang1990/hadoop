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

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.JsonProviderFeature;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import javax.servlet.Filter;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * ResourceManager Web应用入口，负责Web服务的初始化、路由配置和高可用跳转处理
 */
public class RMWebApp extends WebApp implements YarnWebParams {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMWebApp.class.getName());
  private final ResourceManager rm;
  private boolean standby = false;
  private Configuration conf;

  /**
   * 构造RM Web应用实例，关联对应的ResourceManager
   * @param rm 所属ResourceManager实例
   */
  public RMWebApp(ResourceManager rm) {
    this.rm = rm;
  }

  /**
   * 构建Jersey资源配置，注册REST服务和组件
   * @param config 配置对象
   * @return 构建完成的Jersey资源配置
   */
  public ResourceConfig resourceConfig(Configuration config) {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(new JerseyBinder());

    // 加载自定义Web服务类，默认使用RMWebServices
    Class webService = config.getClass(YarnConfiguration.YARN_WEBAPP_CUSTOM_WEBSERVICE_CLASS,
        RMWebServices.class);
    resourceConfig.register(webService);
    LOG.debug("Registered webservice class is {}", webService.getName());

    resourceConfig.register(GenericExceptionHandler.class);
    resourceConfig.register(JsonProviderFeature.class);
    resourceConfig.register(JAXBContextResolver.class);
    return resourceConfig;
  }

  /**
   * Jersey依赖注入绑定器，绑定ResourceManager和配置对象到注入容器
   */
  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      bind(rm).to(ResourceManager.class).named("rm");
      bind(rm.getConfig()).to(Configuration.class).named("conf");
    }
  }

  /**
   * Web应用初始化，绑定依赖并配置所有请求路由
   */
  @Override
  public void setup() {
    conf = rm.getConfig();

    bind(RMWebApp.class).toInstance(this);
    // 绑定用户配置的外部扩展类
    bindExternalClasses();
    bind(ResourceManager.class).toInstance(rm);

    // 配置各个页面的路由规则
    route("/", RmController.class);
    route(pajoin("/nodes", NODE_STATE), RmController.class, "nodes");
    route(pajoin("/apps", APP_STATE), RmController.class);
    route("/cluster", RmController.class, "about");
    route(pajoin("/app", APPLICATION_ID), RmController.class, "app");
    route("/scheduler", RmController.class, "scheduler");
    route(pajoin("/queue", QUEUE_NAME), RmController.class, "queue");
    route("/nodelabels", RmController.class, "nodelabels");
    route(pajoin("/appattempt", APPLICATION_ATTEMPT_ID), RmController.class,
      "appattempt");
    route(pajoin("/container", CONTAINER_ID), RmController.class, "container");
    route("/errors-and-warnings", RmController.class, "errorsAndWarnings");
    route(pajoin("/logaggregationstatus", APPLICATION_ID),
      RmController.class, "logaggregationstatus");
    route(pajoin("/failure", APPLICATION_ID), RmController.class, "failure");
  }

  /**
   * 获取Web应用过滤器类
   * @return RMWebAppFilter过滤器类
   */
  @Override
  protected Class<? extends Filter> getWebAppFilterClass() {
    return RMWebAppFilter.class;
  }

  /**
   * 检查当前RM是否处于Standby状态，更新本地标记
   */
  public void checkIfStandbyRM() {
    standby = (rm.getRMContext().getHAServiceState() == HAServiceState.STANDBY);
  }

  /**
   * 获取当前RM是否为Standby状态
   * @return true表示当前为Standby，false表示为Active
   */
  public boolean isStandby() {
    return standby;
  }

  /**
   * 获取重定向路径，Standby节点会重定向到Active RM
   * @return 重定向目标路径
   */
  @Override
  public String getRedirectPath() {
    if (standby) {
      return buildRedirectPath();
    } else
      return super.getRedirectPath();
  }

  /**
   * 绑定配置文件中指定的外部扩展类到Web容器
   */
  private void bindExternalClasses() {
    Class<?>[] externalClasses = conf
        .getClasses(YarnConfiguration.YARN_HTTP_WEBAPP_EXTERNAL_CLASSES);
    for (Class<?> c : externalClasses) {
      bind(c);
    }
  }


  /**
   * 构建Active RM的重定向地址，用于HA场景下Standby节点跳转
   * @return Active RM的完整HTTP/HTTPS地址
   */
  private String buildRedirectPath() {
    // 复制原始配置避免修改原对象，新建YarnConfiguration确保加载yarn-site.xml
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    // 从配置中查找当前Active RM的ID
    String activeRMHAId = RMHAUtils.findActiveRMHAId(yarnConf);
    String path = "";
    if (activeRMHAId != null) {
      yarnConf.set(YarnConfiguration.RM_HA_ID, activeRMHAId);

      // 根据HTTPS配置获取对应Active RM的Web服务地址
      InetSocketAddress sock = YarnConfiguration.useHttps(yarnConf)
          ? yarnConf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT)
          : yarnConf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);

      // 拼接完整URL地址
      path = sock.getHostName() + ":" + sock.getPort();
      path = YarnConfiguration.useHttps(yarnConf)
          ? "https://" + path
          : "http://" + path;
    }
    return path;
  }

  /**
   * 获取HA Zookeeper连接状态，用于页面展示
   * @return Zookeeper连接状态字符串
   */
  public String getHAZookeeperConnectionState() {
    return getRMContext().getHAZookeeperConnectionState();
  }

  /**
   * 获取当前RM上下文对象
   * @return RM上下文
   */
  public RMContext getRMContext() {
    return rm.getRMContext();
  }
}