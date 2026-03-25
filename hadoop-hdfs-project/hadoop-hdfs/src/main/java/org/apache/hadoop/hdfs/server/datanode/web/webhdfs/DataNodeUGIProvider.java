// 这个文件已经全部加上中文注释
/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 为DataNode上的WebHDFS请求从请求参数中创建用户组信息(UGI)。
 * 注意：DataNode本身不对UGI做认证，后续操作由NameNode负责认证。
 * 核心职责是解析请求中的用户信息或代理token，缓存生成的UGI提升性能。
 */
public class DataNodeUGIProvider {
  private final ParameterParser params;
  @VisibleForTesting
  static Cache<String, UserGroupInformation> ugiCache;
  public static final Logger LOG = LoggerFactory.getLogger(Client.class);

  /**
   * 构造UGI提供器，绑定请求参数解析器
   * @param params Web请求参数解析器实例
   */
  DataNodeUGIProvider(ParameterParser params) {
    this.params = params;
  }

  /**
   * 初始化UGI缓存，根据配置设置缓存过期时间
   * 同步方法保证线程安全，避免重复初始化
   * @param conf Hadoop配置对象
   */
  public static synchronized void init(Configuration conf) {
    if (ugiCache == null) {
      ugiCache = CacheBuilder
          .newBuilder()
          .expireAfterAccess(
              conf.getInt(
                  DFSConfigKeys.DFS_WEBHDFS_UGI_EXPIRE_AFTER_ACCESS_KEY,
                  DFSConfigKeys.DFS_WEBHDFS_UGI_EXPIRE_AFTER_ACCESS_DEFAULT),
              TimeUnit.MILLISECONDS).build();
    }
  }

  /**
   * 清空UGI缓存中当前DelegationToken相关的缓存
   * 仅用于测试场景
   * @throws IOException 清空缓存时抛出IO异常
   */
  @VisibleForTesting
  void clearCache() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      params.delegationToken().decodeIdentifier().clearCache();
    }
  }

  /**
   * 根据请求参数获取解析后的用户组信息UGI，优先从缓存获取
   * 支持DelegationToken认证和用户名代理两种场景
   * @return 解析完成的UGI对象
   * @throws IOException 解析或缓存获取失败时抛出IO异常
   */
  UserGroupInformation ugi() throws IOException {
    UserGroupInformation ugi;

    try {
      final Token<DelegationTokenIdentifier> token = params.delegationToken();

      // 无论安全模式是否开启，token为空时都创建非token模式UGI
      // 支持安全模式DataNode通过非安全模式NameNode访问数据的场景
      if (UserGroupInformation.isSecurityEnabled() && token != null) {
        ugi = ugiCache.get(buildTokenCacheKey(token),
            new Callable<UserGroupInformation>() {
              @Override
              public UserGroupInformation call() throws Exception {
                return tokenUGI(token);
              }
            });
      } else {
        final String usernameFromQuery = params.userName();
        final String doAsUserFromQuery = params.doAsUser();
        final String remoteUser = usernameFromQuery == null ? JspHelper
            .getDefaultWebUserName(params.conf()) // 请求中未指定用户名时使用默认值
            : usernameFromQuery;

        ugi = ugiCache.get(
            buildNonTokenCacheKey(doAsUserFromQuery, remoteUser),
            new Callable<UserGroupInformation>() {
              @Override
              public UserGroupInformation call() throws Exception {
                return nonTokenUGI(usernameFromQuery, doAsUserFromQuery,
                    remoteUser);
              }
            });
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException(cause);
      }
    }

    return ugi;
  }

  /**
   * 基于DelegationToken构建缓存键
   * @param token DelegationToken对象
   * @return 缓存键字符串
   */
  private String buildTokenCacheKey(Token<DelegationTokenIdentifier> token) {
    return token.buildCacheKey();
  }

  /**
   * 从DelegationToken解析并创建UGI对象
   * @param token DelegationToken对象
   * @return 包含token信息的UGI对象
   * @throws IOException 解析token标识符失败时抛出IO异常
   */
  private UserGroupInformation tokenUGI(Token<DelegationTokenIdentifier> token)
      throws IOException {
    ByteArrayInputStream buf =
      new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    id.readFields(in);
    UserGroupInformation ugi = id.getUser();
    ugi.addToken(token);
    return ugi;
  }

  /**
   * 为非token模式UGI构建缓存键，包含远程用户和代理用户信息
   * @param doAsUserFromQuery 请求中的代理用户名
   * @param remoteUser 原始远程用户名
   * @return 缓存键字符串
   * @throws IOException 构建键时抛出IO异常
   */
  private String buildNonTokenCacheKey(String doAsUserFromQuery,
      String remoteUser) throws IOException {
    String key = doAsUserFromQuery == null ? String.format("{%s}", remoteUser)
        : String.format("{%s}:{%s}", remoteUser, doAsUserFromQuery);
    return key;
  }

  /**
   * 创建非token模式的UGI对象，支持代理用户场景
   * 仅用于测试场景
   * @param usernameFromQuery 请求中的用户名
   * @param doAsUserFromQuery 请求中的代理用户名
   * @param remoteUser 原始远程用户名
   * @return 创建完成的UGI对象
   * @throws IOException 用户名校验失败时抛出IO异常
   */
  @VisibleForTesting
  UserGroupInformation nonTokenUGI(String usernameFromQuery,
      String doAsUserFromQuery, String remoteUser) throws IOException {

    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser(remoteUser);
    JspHelper.checkUsername(ugi.getShortUserName(), usernameFromQuery);
    if (doAsUserFromQuery != null) {
      // 创建代理用户UGI并完成授权校验
      ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, ugi);
    }
    return ugi;
  }
}