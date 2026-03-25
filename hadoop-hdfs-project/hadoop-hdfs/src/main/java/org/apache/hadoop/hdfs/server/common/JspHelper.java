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

package org.apache.hadoop.hdfs.server.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authorize.ProxyServers;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER;

/**
 * HDFS Web页面JSP辅助工具类，为HTTP请求处理提供用户身份认证、地址解析等通用能力，
 * 支撑HDFS Web UI的安全认证与用户信息获取。
 */
@InterfaceAudience.Private
public class JspHelper {
  // ServletContext中存储当前配置的属性名
  public static final String CURRENT_CONF = "current.conf";
  // URL参数中委托令牌的参数名
  public static final String DELEGATION_PARAMETER_NAME = DelegationParam.NAME;
  // URL参数中NameNode地址的参数名
  public static final String NAMENODE_ADDRESS = "nnaddr";
  private static final Logger LOG = LoggerFactory.getLogger(JspHelper.class);

  /** 私有构造函数，防止实例化工具类。 */
  private JspHelper() {}

  /**
   * 从配置中获取Web访问默认用户名，用于非安全模式下的默认身份。
   * @param conf Hadoop配置对象
   * @return 默认Web用户名
   * @throws IOException 配置中未配置有效用户名时抛出异常
   */
  public static String getDefaultWebUserName(Configuration conf) throws IOException {
    String user = conf.get(
        HADOOP_HTTP_STATIC_USER, DEFAULT_HADOOP_HTTP_STATIC_USER);
    if (user == null || user.length() == 0) {
      throw new IOException("Cannot determine UGI from request or conf");
    }
    return user;
  }

  /**
   * 从HTTP请求或Servlet上下文获取NameNode服务地址。
   * @param context Servlet上下文对象
   * @param request HTTP请求对象
   * @return NameNode服务地址，获取失败返回null
   */
  private static InetSocketAddress getNNServiceAddress(ServletContext context,
      HttpServletRequest request) {
    String namenodeAddressInUrl = request.getParameter(NAMENODE_ADDRESS);
    InetSocketAddress namenodeAddress = null;
    if (namenodeAddressInUrl != null) {
      // 优先从URL参数解析NameNode地址
      namenodeAddress = NetUtils.createSocketAddr(namenodeAddressInUrl);
    } else if (context != null) {
      // 从Servlet上下文获取NameNode地址
      namenodeAddress = NameNodeHttpServer.getNameNodeAddressFromContext(
          context); 
    }
    if (namenodeAddress != null) {
      return namenodeAddress;
    }
    return null;
  }

  /** 与getUGI(null, request, conf)功能一致，重载方法 */
  public static UserGroupInformation getUGI(HttpServletRequest request,
      Configuration conf) throws IOException {
    return getUGI(null, request, conf);
  }
  
  /** 与getUGI(context, request, conf, KERBEROS_SSL, true)功能一致，重载方法 */
  public static UserGroupInformation getUGI(ServletContext context,
      HttpServletRequest request, Configuration conf) throws IOException {
    return getUGI(context, request, conf, AuthenticationMethod.KERBEROS_SSL, true);
  }

  /**
   * 从HTTP请求中解析并获取用户组信息（UserGroupInformation），支持委托令牌认证、代理用户等多种认证方式。
   * @param context 处理请求的Servlet上下文
   * @param request HTTP请求对象
   * @param conf Hadoop配置对象
   * @param secureAuthMethod 安全模式下使用的认证方式
   * @param tryUgiParameter 是否尝试从ugi参数解析用户名
   * @return 从请求中解析得到的用户组信息
   * @throws IOException 认证失败或获取用户信息出错时抛出
   */
  public static UserGroupInformation getUGI(ServletContext context,
      HttpServletRequest request, Configuration conf,
      final AuthenticationMethod secureAuthMethod,
      final boolean tryUgiParameter) throws IOException {
    UserGroupInformation ugi = null;
    // 从请求参数中获取用户名
    final String usernameFromQuery = getUsernameFromQuery(request, tryUgiParameter);
    // 从请求参数获取代理用户名
    final String doAsUserFromQuery = request.getParameter(DoAsParam.NAME);
    final String remoteUser;
   
    if (UserGroupInformation.isSecurityEnabled()) {
      // 安全模式开启，获取过滤器认证后的远程用户
      remoteUser = request.getRemoteUser();
      // 获取请求中的委托令牌参数
      final String tokenString = request.getParameter(DELEGATION_PARAMETER_NAME);
      if (tokenString != null) {
        // 使用委托令牌进行认证，忽略URL中的用户名和doAs参数
        ugi = getTokenUGI(context, request, tokenString, conf);
      } else if (remoteUser == null) {
        // 安全模式开启但未完成用户认证
        throw new IOException(
            "Security enabled but user not authenticated by filter");
      }
    } else {
      // 安全模式关闭，从URL获取用户名或使用默认Web用户
      remoteUser = (usernameFromQuery == null)
          ? getDefaultWebUserName(conf) // 请求中未指定用户名，使用默认值
          : usernameFromQuery;
    }

    if (ugi == null) { // 无委托令牌的场景（安全模式无令牌或非安全模式）
      // 创建远程用户对象
      ugi = UserGroupInformation.createRemoteUser(remoteUser);
      if (UserGroupInformation.isSecurityEnabled()) {
        // 设置认证方法
        ugi.setAuthenticationMethod(secureAuthMethod);
      }
      if (doAsUserFromQuery != null && !doAsUserFromQuery.equals(remoteUser)) {
        // 请求要求代理到其他用户，创建代理用户并进行授权检查
        ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, ugi);
        ProxyUsers.authorize(ugi, getRemoteAddr(request));
      }
    }
    
    if(LOG.isDebugEnabled())
      LOG.debug("getUGI is returning: " + ugi.getShortUserName());
    return ugi;
  }

  /**
   * 解析并验证HTTP请求中的HDFS委托令牌，生成对应用户组信息。
   * @param context Servlet上下文对象
   * @param request HTTP请求对象
   * @param tokenString 委托令牌的URL编码字符串
   * @param conf Hadoop配置对象
   * @return 委托令牌对应的用户组信息
   * @throws IOException 令牌解析或验证失败时抛出异常
   */
  private static UserGroupInformation getTokenUGI(ServletContext context,
                                                  HttpServletRequest request,
                                                  String tokenString,
                                                  Configuration conf)
                                                      throws IOException {
    final Token<DelegationTokenIdentifier> token =
        new Token<DelegationTokenIdentifier>();
    // 从URL编码字符串解码令牌
    token.decodeFromUrlString(tokenString);
    // 获取NameNode服务地址
    InetSocketAddress serviceAddress = getNNServiceAddress(context, request);
    if (serviceAddress != null) {
      // 设置令牌的服务地址
      SecurityUtil.setTokenService(token, serviceAddress);
      // 设置令牌类型为HDFS委托令牌
      token.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
    }

    ByteArrayInputStream buf =
        new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    // 反序列化令牌标识符
    id.readFields(in);
    if (context != null) {
      // 从Servlet上下文获取令牌验证器
      final TokenVerifier<DelegationTokenIdentifier> tokenVerifier =
          NameNodeHttpServer.getTokenVerifierFromContext(context);
      if (tokenVerifier != null) {
        // 验证令牌签名
        tokenVerifier.verifyToken(id, token.getPassword());
      }
    }
    // 从令牌标识符获取对应用户
    UserGroupInformation ugi = id.getUser();
    // 将令牌添加到用户信息中
    ugi.addToken(token);
    return ugi;
  }

  /**
   * 获取请求客户端真实IP地址，支持信任代理的X-Forwarded-For请求头，正确获取经过HTTP代理后的客户端真实地址。
   * @param request HTTP请求对象
   * @return 客户端真实IP地址字符串
   */
  public static String getRemoteAddr(HttpServletRequest request) {
    String remoteAddr = request.getRemoteAddr();
    String proxyHeader = request.getHeader("X-Forwarded-For");
    // 如果请求头存在X-Forwarded-For且当前请求来自信任的代理服务器，则使用X-Forwarded-For中的客户端地址
    if (proxyHeader != null && ProxyServers.isProxyServer(remoteAddr)) {
      // 分割得到第一个地址即为原始客户端地址
      final String clientAddr = proxyHeader.split(",")[0].trim();
      if (!clientAddr.isEmpty()) {
        remoteAddr = clientAddr;
      }
    }
    return remoteAddr;
  }

  /**
   * 获取HTTP请求客户端端口。
   * @param request HTTP请求对象
   * @return 客户端端口号
   */
  public static int getRemotePort(HttpServletRequest request) {
    return request.getRemotePort();
  }

  /**
   * 检查实际用户名与预期用户名是否匹配，将Kerberos用户名转换为短名称后比较。
   * @param expected 预期的短用户名
   * @param name 实际拿到的用户名字符串（可能包含Kerberos域信息）
   * @throws IOException 用户名不匹配时抛出异常
   */
  public static void checkUsername(final String expected, final String name
      ) throws IOException {
    if (expected == null && name != null) {
      throw new IOException("Usernames not matched: expecting null but name="
          + name);
    }
    if (name == null) { //用户名是可选的，null允许
      return;
    }
    KerberosName u = new KerberosName(name);
    // 获取Kerberos用户名的短名称部分（去掉域信息）
    String shortName = u.getShortName();
    if (!shortName.equals(expected)) {
      throw new IOException("Usernames not matched: name=" + shortName
          + " != expected=" + expected);
    }
  }

  /**
   * 从HTTP请求查询参数中解析用户名，支持User参数和传统ugi参数两种格式。
   * @param request HTTP请求对象
   * @param tryUgiParameter 是否允许尝试从ugi参数解析用户名
   * @return 解析得到的用户名，未找到返回null
   */
  private static String getUsernameFromQuery(final HttpServletRequest request,
      final boolean tryUgiParameter) {
    String username = request.getParameter(UserParam.NAME);
    if (username == null && tryUgiParameter) {
      // 尝试从传统ugi参数解析，ugi格式为用户名,组1,组2...，取第一个部分作为用户名
      final String ugiStr = request.getParameter("ugi");
      if (ugiStr != null) {
        username = ugiStr.split(",")[0];
      }
    }
    return username;
  }

}