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

import static javax.servlet.http.HttpServletResponse.SC_NO_CONTENT;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices.DELEGATION_TOKEN_HEADER;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.thirdparty.com.google.common.net.HttpHeaders;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionInfo;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ConflictException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Router Web服务工具类，提供YARN Federation Router跨子集群Web请求转发、结果聚合等公共能力。
 */
public final class RouterWebServiceUtil {

  private static String user = "YarnRouter";

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterWebServiceUtil.class.getName());

  private final static String PARTIAL_REPORT = "Partial Report ";

  /** Disable constructor. */
  private RouterWebServiceUtil() {
  }

  /**
   * 转发REST请求到目标子集群RM，并返回聚合后的结果。
   *
   * @param webApp 远程Web服务地址
   * @param hsr 原始Servlet请求
   * @param returnType REST响应返回类型
   * @param <T> 返回对象泛型
   * @param method HTTP请求方法
   * @param targetPath 请求目标路径
   * @param formParam 表单参数
   * @param additionalParam 额外查询参数
   * @param conf 配置对象
   * @param client 复用的Jersey客户端实例
   * @return 远程REST调用返回的结果对象
   */
  protected static <T> T genericForward(final String webApp,
      final HttpServletRequest hsr, final Class<T> returnType,
      final HTTPMethods method, final String targetPath, final Object formParam,
      final Map<String, String[]> additionalParam, Configuration conf,
      Client client) {
    // 获取请求发起用户UGI
    UserGroupInformation callerUGI;

    if (hsr != null) {
      callerUGI = RMWebAppUtil.getCallerUserGroupInformation(hsr, true);
    } else {
      // 无请求时创建默认Router用户
      callerUGI = UserGroupInformation.createRemoteUser(user);
    }

    if (callerUGI == null) {
      LOG.error("Unable to obtain user name, user not authenticated");
      return null;
    }

    try {
      // 以请求用户身份执行转发操作
      return callerUGI.doAs((PrivilegedExceptionAction<T>) () -> {

        Map<String, String[]> paramMap = null;

        // 参数来自请求或额外参数，二者不会同时存在
        if (hsr != null) {
          paramMap = hsr.getParameterMap();
        } else if (additionalParam != null) {
          paramMap = additionalParam;
        }

        // 调用远程RM Web服务
        Response response = RouterWebServiceUtil.invokeRMWebService(
            webApp, targetPath, method, (hsr == null) ? null : hsr.getPathInfo(), paramMap,
            formParam, getMediaTypeFromHttpServletRequest(hsr, returnType), conf, client);

        try {
          if(returnType == Response.class) {
            return returnType.cast(response);
          }

          // 正常返回结果，读取实体对象
          if (response.getStatus() == SC_OK) {
            T t = response.readEntity(returnType);
            return t;
          }

          // 无内容响应，尝试创建空对象返回
          if (response.getStatus() == SC_NO_CONTENT) {
            try {
              return returnType.getConstructor().newInstance();
            } catch (RuntimeException | ReflectiveOperationException e) {
              LOG.error("Cannot create empty entity for {}", returnType, e);
            }
          }

          // 根据响应状态抛出对应异常
          RouterWebServiceUtil.retrieveException(response);
          return null;
        } finally {
          // 非返回Response时关闭响应释放资源
          if (response != null && returnType != Response.class) {
            response.close();
          }
        }
      });
    } catch (InterruptedException e) {
      return null;
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * 实际调用远程ResourceManager Web服务，构造并发送REST请求。
   * @param webApp 远程Web服务地址
   * @param path 请求目标路径
   * @param method HTTP请求方法
   * @param additionalPath 额外请求路径
   * @param queryParams 查询参数集合
   * @param formParam 请求体参数
   * @param mediaType 请求媒体类型
   * @param conf 配置对象
   * @param client 传入的Jersey客户端
   * @return 远程服务响应对象
   */
  @SuppressWarnings("checkstyle:parameternumber")
  private static Response invokeRMWebService(String webApp, String path,
      HTTPMethods method, String additionalPath,
      Map<String, String[]> queryParams, Object formParam, String mediaType,
      Configuration conf, Client client) {
    // 解析远程地址得到套接字地址
    InetSocketAddress socketAddress = NetUtils
        .getConnectAddress(NetUtils.createSocketAddr(webApp));
    // 根据配置选择http/https协议
    String scheme = YarnConfiguration.useHttps(conf) ? "https://" : "http://";
    String webAddress = scheme + socketAddress.getHostName() + ":"
        + socketAddress.getPort();
    // 创建新客户端构建请求
    Client client1 = ClientBuilder.newClient();
    WebTarget webResource = client1.target(webAddress);

    // 拼接请求路径
    if (additionalPath != null && !additionalPath.isEmpty()) {
      webResource = webResource.path(additionalPath);
    } else {
      webResource = webResource.path(path);
    }

    LOG.info("webApp:{}, path:{}, method:{}, additionalPath:{}, queryParams:{}, " +
        "formParam:{}, mediaType:{}, conf:{}", webApp, path, method, additionalPath,
        queryParams, formParam, mediaType, conf);

    // 添加所有查询参数
    if (queryParams != null && !queryParams.isEmpty()) {
      for (Entry<String, String[]> param : queryParams.entrySet()) {
        String[] values = param.getValue();
        for (int i = 0; i < values.length; i++) {
          webResource = webResource.queryParam(param.getKey(), values[i]);
        }
      }
    }

    // 构建请求，设置媒体类型
    Builder builder = webResource.request(mediaType);

    Response response = null;

    try {
      // 根据HTTP方法执行对应请求
      switch (method) {
      case DELETE:
        response = builder.delete(Response.class);
        break;
      case GET:
        response = builder.get(Response.class);
        break;
      case POST:
        response = builder.post(Entity.entity(formParam, mediaType));
        break;
      case PUT:
        response = builder.put(Entity.entity(formParam, mediaType), Response.class);
        break;
      default:
        break;
      }
    } finally {
      client.close();
    }
    return response;
  }

  /**
   * 根据响应状态码解析并抛出对应Web异常。
   * @param response 远程服务响应
   */
  public static void retrieveException(Response response) {
    String serverErrorMsg = response.readEntity(String.class);
    int status = response.getStatus();
    if (status == 400) {
      throw new BadRequestException(serverErrorMsg);
    }
    if (status == 403) {
      throw new ForbiddenException(serverErrorMsg);
    }
    if (status == 404) {
      throw new NotFoundException(serverErrorMsg);
    }
    if (status == 409) {
      throw new ConflictException(serverErrorMsg);
    }
  }

  /**
   * 合并来自多个子集群的应用信息，按应用ID分组聚合结果。
   * 合并主AM和跨子集群UAM的资源统计信息。
   *
   * @param appsInfo 多个子集群返回的AppInfo列表
   * @param returnPartialResult 是否允许返回缺少主AM的部分结果
   * @return 合并完成的AppsInfo对象
   */
  public static AppsInfo mergeAppsInfo(ArrayList<AppInfo> appsInfo,
      boolean returnPartialResult) {
    AppsInfo allApps = new AppsInfo();

    // 存储包含主AM的应用
    Map<String, AppInfo> federationAM = new HashMap<>();
    // 存储仅包含UAM的应用，等待主AM出现后合并
    Map<String, AppInfo> federationUAMSum = new HashMap<>();
    for (AppInfo a : appsInfo) {
      // 判断当前AppInfo是否包含AM信息
      if (a.getAMHostHttpAddress() != null) {
        // 添加到AM列表
        federationAM.put(a.getAppId(), a);
        // 如果之前已经收集到同应用的UAM，进行合并
        if (federationUAMSum.containsKey(a.getAppId())) {
          mergeAMWithUAM(a, federationUAMSum.get(a.getAppId()));
          // 移除UAM缓存
          federationUAMSum.remove(a.getAppId());
        }
      } else {
        // 当前AppInfo是UAM
        if (federationAM.containsKey(a.getAppId())) {
          // 已经存在AM，直接合并
          mergeAMWithUAM(federationAM.get(a.getAppId()), a);
        } else if (federationUAMSum.containsKey(a.getAppId())) {
          // 已有同应用UAM，合并UAM
          federationUAMSum.put(a.getAppId(),
              mergeUAMWithUAM(federationUAMSum.get(a.getAppId()), a));
        } else {
          // 第一个UAM，加入缓存等待后续合并
          federationUAMSum.put(a.getAppId(), a);
        }
      }
    }

    // 处理剩余未合并的UAM，根据配置决定是否返回
    for (AppInfo a : federationUAMSum.values()) {
      if (returnPartialResult || (a.getName() != null
          && !(a.getName().startsWith(UnmanagedApplicationManager.APP_NAME)
              || a.getName().startsWith(PARTIAL_REPORT)))) {
        federationAM.put(a.getAppId(), a);
      }
    }

    allApps.addAll(new ArrayList<>(federationAM.values()));
    return allApps;
  }

  /**
   * 根据配置创建带超时设置的Jersey客户端。
   * @param conf 配置对象
   * @return 初始化完成的Jersey客户端
   */
  protected static Client createJerseyClient(Configuration conf) {
    Client client = ClientBuilder.newClient();

    // 读取连接超时配置，验证合法性
    long checkConnectTimeOut = conf.getLong(YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, 0);
    int connectTimeOut = (int) conf.getTimeDuration(YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    if (checkConnectTimeOut <= 0 || checkConnectTimeOut > Integer.MAX_VALUE) {
      LOG.warn("Configuration {} = {} ms error. We will use the default value({} ms).",
          YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, connectTimeOut,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT);
      connectTimeOut = (int) TimeUnit.MILLISECONDS.convert(
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    }
    client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeOut);

    // 读取读取超时配置，验证合法性
    long checkReadTimeout = conf.getLong(YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT, 0);
    int readTimeout = (int) conf.getTimeDuration(YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_READ_TIMEOUT, TimeUnit.MILLISECONDS);

    if (checkReadTimeout < 0 || checkReadTimeout > Integer.MAX_VALUE) {
      LOG.warn("Configuration {} = {} ms error. We will use the default value({} ms).",
          YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, connectTimeOut,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT);
      readTimeout = (int) TimeUnit.MILLISECONDS.convert(
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    }
    client.property(ClientProperties.READ_TIMEOUT, readTimeout);

    return client;
  }

  /**
   * 合并两个无主AM的UAM信息，生成部分报告对象。
   * @param uam1 第一个UAM信息
   * @param uam2 第二个UAM信息
   * @return 合并后的部分应用报告对象
   */
  private static AppInfo mergeUAMWithUAM(AppInfo uam1, AppInfo uam2) {
    AppInfo partialReport = new AppInfo();
    partialReport.setAppId(uam1.getAppId());
    partialReport.setName(PARTIAL_REPORT + uam1.getAppId());
    // 使用第一个UAM的状态
    partialReport.setState(uam1.getState());
    // 分别合并两个UAM的资源信息
    mergeAMWithUAM(partialReport, uam1);
    mergeAMWithUAM(partialReport, uam2);
    return partialReport;
  }

  /**
   * 将UAM的资源统计信息合并到主AM的AppInfo中。
   * @param am 主AM应用信息
   * @param uam 要合并的UAM应用信息
   */
  private static void mergeAMWithUAM(AppInfo am, AppInfo uam) {
    // 累加抢占资源统计
    am.setPreemptedResourceMB(
        am.getPreemptedResourceMB() + uam.getPreemptedResourceMB());
    am.setPreemptedResourceVCores(
        am.getPreemptedResourceVC