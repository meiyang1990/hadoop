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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import org.apache.hadoop.yarn.server.nodemanager.webapp.jsonprovider.NMJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import javax.servlet.Filter;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NodeManager Web UI 服务，提供 NodeManager 状态监控和容器管理的HTTP访问入口
 */
public class WebServer extends AbstractService {

  private static final Logger LOG =
       LoggerFactory.getLogger(WebServer.class);

  private final Context nmContext;
  private final NMWebApp nmWebApp;
  private final ResourceView resourceView;
  private WebApp webApp;
  private int port;

  /**
   * 构造NodeManager Web服务器实例
   * @param nmContext NodeManager上下文对象
   * @param resView NodeManager资源视图
   * @param aclsManager 应用访问权限管理器
   * @param dirsHandler 本地目录处理器
   */
  public WebServer(Context nmContext, ResourceView resView,
      ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    super(WebServer.class.getName());
    this.nmContext = nmContext;
    this.nmWebApp = new NMWebApp(resView, aclsManager, dirsHandler);
    this.resourceView = resView;
  }

  /**
   * 配置Jersey REST资源
   * @return 配置完成的Jersey资源配置对象
   */
  protected ResourceConfig configure() {
    NMJsonProvider nmJsonProvider = new NMJsonProvider();

    ResourceConfig config = new ResourceConfig();
    // 扫描webapp包下的REST资源
    config.packages("org.apache.hadoop.yarn.server.nodemanager.webapp");
    // 注册依赖注入绑定
    config.register(new JerseyBinder());
    // 注册REST服务类
    config.register(NMWebServices.class);
    // 注册全局异常处理器
    config.register(GenericExceptionHandler.class);
    // 注册NodeManager JSON序列化提供者
    config.register(nmJsonProvider);
    // 注册JAXB上下文解析器
    config.register(JAXBContextResolver.class);
    return config;
  }

  /**
   * Jersey依赖注入绑定类，将NodeManager核心对象注入到REST资源
   */
  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      bind(nmContext).to(Context.class).named("nm");
      bind(nmWebApp).to(WebApp.class).named("webapp");
      bind(resourceView).to(ResourceView.class).named("view");
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    Map<String, String> params = new HashMap<>();
    // 终端前端资源参数配置
    Map<String, String> terminalParams = new HashMap<>();
    terminalParams.put("resourceBase", WebServer.class
        .getClassLoader().getResource("TERMINAL").toExternalForm());
    terminalParams.put("dirAllowed", "false");
    terminalParams.put("pathInfoOnly", "true");
    // 获取Web服务绑定地址
    String bindAddress = WebAppUtils.getWebAppBindURL(conf,
        YarnConfiguration.NM_BIND_HOST, WebAppUtils.getNMWebAppURLWithoutScheme(conf));
    // 判断是否启用跨域资源共享
    boolean enableCors = conf
        .getBoolean(YarnConfiguration.NM_WEBAPP_ENABLE_CORS_FILTER,
            YarnConfiguration.DEFAULT_NM_WEBAPP_ENABLE_CORS_FILTER);
    if (enableCors) {
      getConfig().setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }

    //  Always load pseudo authentication filter to parse "user.name" in an URL
    //  to identify a HTTP request's user.
    // 检查配置中是否已添加认证过滤器初始化器
    boolean hasHadoopAuthFilterInitializer = false;
    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    Class<?>[] initializersClasses = conf.getClasses(filterInitializerConfKey);
    List<String> targets = new ArrayList<>();
    if (initializersClasses != null) {
      for (Class<?> initializer : initializersClasses) {
        if (initializer.getName().equals(
            AuthenticationFilterInitializer.class.getName())) {
          hasHadoopAuthFilterInitializer = true;
          break;
        }
        targets.add(initializer.getName());
      }
    }
    // 如果未添加认证过滤器，则强制添加
    if (!hasHadoopAuthFilterInitializer) {
      targets.add(AuthenticationFilterInitializer.class.getName());
      conf.set(filterInitializerConfKey, StringUtils.join(",", targets));
    }
    // 初始化容器Shell WebSocket
    ContainerShellWebSocket.init(nmContext);
    LOG.info("Instantiating NMWebApp at {}.", bindAddress);
    try {
      // 构建并启动Web应用
      this.webApp = WebApps
          .$for("node", Context.class, this.nmContext, "jersey-ws")
          .at(bindAddress)
          // 注册容器Shell WebSocket Servlet
          .withServlet("ContainerShellWebSocket", "/container/*",
           ContainerShellWebSocketServlet.class, params, false)
          // 注册终端前端Servlet
          .withServlet("Terminal", "/terminal/*",
           TerminalServlet.class, terminalParams, false)
          .with(conf)
          // 配置SPNEGO认证密钥信息
          .withHttpSpnegoPrincipalKey(YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY)
          .withHttpSpnegoKeytabKey(YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
          // 启用CSRF防护
          .withCSRFProtection(YarnConfiguration.NM_CSRF_PREFIX)
          // 启用XFS防护
          .withXFSProtection(YarnConfiguration.NM_XFS_PREFIX)
          // 设置Jersey资源配置
          .withResourceConfig(configure())
          .start(this.nmWebApp);
      // 获取实际绑定的端口号
      this.port = this.webApp.httpServer().getConnectorAddress(0).getPort();
    } catch (Exception e) {
      String msg = "NMWebapps failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
    super.serviceStart();
  }

  /**
   * 获取Web服务实际绑定的端口号
   * @return 端口号
   */
  public int getPort() {
    return this.port;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.webApp != null) {
      LOG.debug("Stopping webapp");
      this.webApp.stop();
    }
    super.serviceStop();
  }

  /**
   * NodeManager Web应用，负责页面路由和依赖绑定
   */
  public static class NMWebApp extends WebApp implements YarnWebParams {

    private final ResourceView resourceView;
    private final ApplicationACLsManager aclsManager;
    private final LocalDirsHandlerService dirsHandler;

    public NMWebApp(ResourceView resourceView,
        ApplicationACLsManager aclsManager,
        LocalDirsHandlerService dirsHandler) {
      this.resourceView = resourceView;
      this.aclsManager = aclsManager;
      this.dirsHandler = dirsHandler;
    }

    @Override
    public void setup() {
      // 绑定核心服务实例到依赖注入容器
      bind(ResourceView.class).toInstance(this.resourceView);
      bind(ApplicationACLsManager.class).toInstance(this.aclsManager);
      bind(LocalDirsHandlerService.class).toInstance(dirsHandler);
      // 配置页面路由
      route("/", NMController.class, "info");
      route("/node", NMController.class, "node");
      route("/allApplications", NMController.class, "allApplications");
      route("/allContainers", NMController.class, "allContainers");
      route(pajoin("/application", APPLICATION_ID), NMController.class,
          "application");
      route(pajoin("/container", CONTAINER_ID), NMController.class,
          "container");
      route(
          pajoin("/containerlogs", CONTAINER_ID, APP_OWNER, CONTAINER_LOG_TYPE),
          NMController.class, "logs");
      route("/errors-and-warnings", NMController.class, "errorsAndWarnings");
    }

    @Override
    protected Class<? extends Filter> getWebAppFilterClass() {
      return NMWebAppFilter.class;
    }
  }
}