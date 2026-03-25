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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilterInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CredentialsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LocalResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LogAggregationContextInfo;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilter;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * ResourceManager WebApp 工具类，提供Web服务安全配置、应用提交上下文构造等公共能力。
 */
public final class RMWebAppUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMWebAppUtil.class);

  /**
   * 工具类禁止实例化，私有构造函数。
   */
  private RMWebAppUtil() {
    // not called
  }

  /**
   * 为ResourceManager Web服务配置安全认证与过滤器链，支持代理用户、委派令牌认证等特性。
   * 只有同时满足四个条件才会替换使用RM自定义认证过滤器：
   * 1. 集群安全模式已启用
   * 2. HTTP认证类型配置为kerberos
   * 3. 配置启用RM自定义委派令牌认证过滤器
   * 4. 原有过滤器链包含标准Hadoop认证过滤器初始化器
   * 
   * @param conf ResourceManager配置对象
   * @param rmDTSecretManager RM委派令牌密钥管理器，用于认证过滤器验证令牌
   **/
  public static void setupSecurityAndFilters(Configuration conf,
      RMDelegationTokenSecretManager rmDTSecretManager) {

    boolean enableCorsFilter =
        conf.getBoolean(YarnConfiguration.RM_WEBAPP_ENABLE_CORS_FILTER,
            YarnConfiguration.DEFAULT_RM_WEBAPP_ENABLE_CORS_FILTER);
    boolean useYarnAuthenticationFilter = conf.getBoolean(
        YarnConfiguration.RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER,
        YarnConfiguration.DEFAULT_RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER);
    String authPrefix = "hadoop.http.authentication.";
    String authTypeKey = authPrefix + "type";
    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    String actualInitializers = "";
    Class<?>[] initializersClasses = conf.getClasses(filterInitializerConfKey);

    // 配置跨域资源共享过滤器
    if (enableCorsFilter) {
      conf.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }

    boolean hasHadoopAuthFilterInitializer = false;
    boolean hasRMAuthFilterInitializer = false;
    if (initializersClasses != null) {
      // 遍历现有过滤器初始化器，检查是否存在标准认证过滤器和RM认证过滤器
      for (Class<?> initializer : initializersClasses) {
        if (initializer.getName()
            .equals(AuthenticationFilterInitializer.class.getName())) {
          hasHadoopAuthFilterInitializer = true;
        }
        if (initializer.getName()
            .equals(RMAuthenticationFilterInitializer.class.getName())) {
          hasRMAuthFilterInitializer = true;
        }
      }
      // 满足所有替换条件，替换标准Hadoop认证过滤器为RM自定义认证过滤器
      if (UserGroupInformation.isSecurityEnabled()
          && useYarnAuthenticationFilter && hasHadoopAuthFilterInitializer
          && conf.get(authTypeKey, "")
              .equals(KerberosAuthenticationHandler.TYPE)) {
        ArrayList<String> target = new ArrayList<String>();
        for (Class<?> filterInitializer : initializersClasses) {
          if (filterInitializer.getName()
              .equals(AuthenticationFilterInitializer.class.getName())) {
            // 如果还没有RM认证过滤器，添加进去
            if (!hasRMAuthFilterInitializer) {
              target.add(RMAuthenticationFilterInitializer.class.getName());
            }
            continue;
          }
          target.add(filterInitializer.getName());
        }

        // 移除代理用户过滤器，由RM认证过滤器统一处理
        target.remove(ProxyUserAuthenticationFilterInitializer.class.getName());

        // 生成新的过滤器初始化器配置字符串
        actualInitializers = StringUtils.join(",", target);

        LOG.info("Using RM authentication filter(kerberos/delegation-token)"
            + " for RM webapp authentication");
        // 给RM认证过滤器设置委派令牌密钥管理器，用于令牌验证
        RMAuthenticationFilter
            .setDelegationTokenSecretManager(rmDTSecretManager);
        // 更新配置，使用新的过滤器链
        conf.set(filterInitializerConfKey, actualInitializers);
      }
    }

    // 非安全模式下，如果没有配置认证过滤器，添加RM简单认证过滤器
    String initializers = conf.get(filterInitializerConfKey);
    if (!UserGroupInformation.isSecurityEnabled()) {
      if (initializersClasses == null || initializersClasses.length == 0) {
        // 完全没有配置过滤器，直接使用RM认证过滤器
        conf.set(filterInitializerConfKey,
            RMAuthenticationFilterInitializer.class.getName());
        conf.set(authTypeKey, "simple");
      } else if (initializers.equals(StaticUserWebFilter.class.getName())) {
        // 已有静态用户过滤器，追加RM认证过滤器
        conf.set(filterInitializerConfKey,
            RMAuthenticationFilterInitializer.class.getName() + ","
                + initializers);
        conf.set(authTypeKey, "simple");
      }
    }
  }

  /**
   * 根据用户通过WebAPI提交的应用信息，构造可提交给RM的ApplicationSubmissionContext对象。
   *
   * @param newApp 用户通过Web提交的应用信息对象
   * @param conf RM配置对象
   * @return 构造完成的应用提交上下文，可直接提交给RM
   * @throws IOException 解析过程中IO异常
   */
  public static ApplicationSubmissionContext createAppSubmissionContext(
      ApplicationSubmissionContextInfo newApp, Configuration conf)
      throws IOException {

    // 创建本地资源，构造应用提交上下文

    ApplicationId appid;
    String error =
        "Could not parse application id " + newApp.getApplicationId();
    try {
      // 解析应用ID字符串
      appid = ApplicationId.fromString(newApp.getApplicationId());
    } catch (Exception e) {
      throw new BadRequestException(error);
    }
    // 构造应用提交上下文核心对象
    ApplicationSubmissionContext appContext = ApplicationSubmissionContext
        .newInstance(appid, newApp.getApplicationName(), newApp.getQueue(),
            Priority.newInstance(newApp.getPriority()),
            createContainerLaunchContext(newApp), newApp.getUnmanagedAM(),
            newApp.getCancelTokensWhenComplete(), newApp.getMaxAppAttempts(),
            createAppSubmissionContextResource(newApp, conf),
            newApp.getApplicationType(),
            newApp.getKeepContainersAcrossApplicationAttempts(),
            newApp.getAppNodeLabelExpression(),
            newApp.getAMContainerNodeLabelExpression());
    // 设置应用标签
    appContext.setApplicationTags(newApp.getApplicationTags());
    // 设置尝试失败有效间隔
    appContext.setAttemptFailuresValidityInterval(
        newApp.getAttemptFailuresValidityInterval());
    // 如果提供了日志聚合上下文，设置进去
    if (newApp.getLogAggregationContextInfo() != null) {
      appContext.setLogAggregationContext(
          createLogAggregationContext(newApp.getLogAggregationContextInfo()));
    }
    // 如果提供了预留ID，解析后设置进去
    String reservationIdStr = newApp.getReservationId();
    if (reservationIdStr != null && !reservationIdStr.isEmpty()) {
      ReservationId reservationId =
          ReservationId.parseReservationId(reservationIdStr);
      appContext.setReservationID(reservationId);
    }
    return appContext;
  }

  /**
   * 根据用户提交的资源请求信息，构造Resource对象并校验资源请求不超过集群配置的最大值。
   *
   * @param newApp 用户提交的应用信息
   * @param conf RM配置对象
   * @return 校验通过的Resource对象
   * @throws BadRequestException 请求资源超过集群最大值时抛出
   */
  private static Resource createAppSubmissionContextResource(
      ApplicationSubmissionContextInfo newApp, Configuration conf)
      throws BadRequestException {
    // 校验请求vCore数不超过集群最大值
    if (newApp.getResource().getvCores() > conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES)) {
      String msg = "Requested more cores than configured max";
      throw new BadRequestException(msg);
    }
    // 校验请求内存不超过集群最大值
    if (newApp.getResource().getMemorySize() > conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB)) {
      String msg = "Requested more memory than configured max";
      throw new BadRequestException(msg);
    }
    // 构造并返回Resource对象
    Resource r = Resource.newInstance(newApp.getResource().getMemorySize(),
        newApp.getResource().getvCores());
    return r;
  }

  /**
   * 根据用户提交的容器启动信息，构造ContainerLaunchContext对象，处理Base64编码的凭证和辅助服务数据。
   *
   * @param newApp 用户提交的应用信息
   * @return 构造完成的容器启动上下文
   * @throws BadRequestException 解析数据失败时抛出
   * @throws IOException IO处理异常时抛出
   */
  private static ContainerLaunchContext createContainerLaunchContext(
      ApplicationSubmissionContextInfo newApp)
      throws BadRequestException, IOException {

    // 构造容器启动上下文

    // 处理辅助服务数据，解码Base64为ByteBuffer
    HashMap<String, ByteBuffer> hmap = new HashMap<String, ByteBuffer>();
    for (Map.Entry<String, String> entry : newApp
        .getContainerLaunchContextInfo().getAuxillaryServiceData().entrySet()) {
      if (!entry.getValue().isEmpty()) {
        Base64 decoder = new Base64(0, null, true);
        byte[] data = decoder.decode(entry.getValue());
        hmap.put(entry.getKey(), ByteBuffer.wrap(data));
      }
    }

    // 处理本地资源，转换为LocalResource对象
    HashMap<String, LocalResource> hlr = new HashMap<String, LocalResource>();
    for (Map.Entry<String, LocalResourceInfo> entry : newApp
        .getContainerLaunchContextInfo().getResources().entrySet()) {
      LocalResourceInfo l = entry.getValue();
      LocalResource lr = LocalResource.newInstance(URL.fromURI(l.getUrl()),
          l.getType(), l.getVisibility(), l.getSize(), l.getTimestamp());
      hlr.put(entry.getKey(), lr);
    }

    // 序列化凭证到ByteBuffer，供容器使用
    DataOutputBuffer out = new DataOutputBuffer();
    Credentials cs = createCredentials(
        newApp.getContainerLaunchContextInfo().getCredentials());
    cs.writeTokenStorageToStream(out);
    ByteBuffer tokens = ByteBuffer.wrap(out.getData());

    // 构造并返回容器启动上下文对象
    ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(hlr,
        newApp.getContainerLaunchContextInfo().getEnvironment(),
        newApp.getContainerLaunchContextInfo().getCommands(), hmap, tokens,
        newApp.getContainerLaunchContextInfo().getAcls());

    return ctx;
  }

  /**
   * 根据用户提交的凭证信息，构造Credentials对象，解码Base64编码的令牌和密钥。
   *
   * @param credentials 用户提交的凭证信息对象
   * @return 构造完成的Credentials对象
   */
  private static Credentials createCredentials(CredentialsInfo credentials) {
    Credentials ret = new Credentials();
    try {
      // 解析令牌信息，从URL安全字符串解码
      for (Map.Entry<String, String> entry : credentials.getTokens()
          .entrySet()) {
        Text alias = new Text(entry.getKey());
        Token<TokenIdentifier> token = new Token<TokenIdentifier>();
        token.decodeFromUrlString(entry.getValue());
        ret.addToken(alias, token);
      }
      // 解析密钥，从Base64编码解码
      for (Map.Entry<String, String> entry : credentials.getSecrets()
          .entrySet()) {
        Text alias = new Text(entry.getKey());
        Base64 decoder = new Base64(0, null, true);
        byte[] secret = decoder.decode(entry.getValue());
        ret.addSecretKey(alias, secret);
      }
    } catch (IOException ie) {
      throw new BadRequestException(
          "Could not parse credentials data; exception message = "
              + ie.getMessage());
    }
    return ret;
  }

  /**
   * 根据用户提交的日志聚合上下文信息，构造LogAggregationContext对象。
   *
   * @param logAggregationContextInfo 用户提交的日志聚合上下文信息
   * @return 构造完成的日志聚合上下文对象
   */
  private static LogAggregationContext createLogAggregationContext(
      LogAggregationContextInfo logAggregationContextInfo) {
    return LogAggregationContext.newInstance(
        logAggregationContextInfo.getIncludePattern(),
        logAggregationContextInfo.getExcludePattern(),
        logAggregationContextInfo.getRolledLogsIncludePattern(),
        logAggregationContextInfo.getRolledLogsExcludePattern(),
        logAggregationContextInfo.getLogAggregationPolicyClassName(),
        logAggregationContextInfo.getLogAggregationPolicyParameters());
  }

  /**
   * 从HttpServletRequest中提取请求发起者的UserGroupInformation对象。
   *
   * @param hsr HTTP请求对象
   * @param usePrincipal 是否优先使用Principal获取用户名，否则使用remoteUser
   * @return 请求发起者的UGI对象，如果无法获取用户名返回null
   **/
  public static UserGroupInformation getCallerUserGroupInformation(
      HttpServletRequest hsr, boolean usePrincipal) {

    String remoteUser = hsr.getRemoteUser();
    // 如果需要使用Principal获取用户名，从请求中提取
    if (usePrincipal) {
      Principal princ = hsr.getUserPrincipal();
      remoteUser = princ == null ? null : princ.getName();
    }

    UserGroupInformation callerUGI = null;
    // 如果成功获取到用户名，创建远程用户UGI
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }

    return callerUGI;
  }
}