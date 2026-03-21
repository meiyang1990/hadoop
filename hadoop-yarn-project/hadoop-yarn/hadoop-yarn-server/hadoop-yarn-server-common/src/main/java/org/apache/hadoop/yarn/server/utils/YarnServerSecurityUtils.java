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

package org.apache.hadoop.yarn.server.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN服务端安全相关工具类，提供AMRMToken处理、请求认证、凭证解析等常用安全能力。
 *
 */
@Private
public final class YarnServerSecurityUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(YarnServerSecurityUtils.class);

  private YarnServerSecurityUtils() {
  }

  /**
   * 对当前ApplicationMaster请求进行认证，提取并返回当前应用的AMRMTokenIdentifier。
   *
   * @return 当前对应用户的AMRMTokenIdentifier实例
   * @throws YarnException 认证失败时抛出YARN异常
   */
  public static AMRMTokenIdentifier authorizeRequest() throws YarnException {

    UserGroupInformation remoteUgi;
    try {
      // 获取当前请求对应用户UGI
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg =
          "Cannot obtain the user-name for authorizing ApplicationMaster. "
              + "Got exception: " + StringUtils.stringifyException(e);
      LOG.warn(msg);
      // 封装IO异常为远程RPC异常返回
      throw RPCUtil.getRemoteException(msg);
    }

    boolean tokenFound = false;
    String message = "";
    AMRMTokenIdentifier appTokenIdentifier = null;
    try {
      // 从UGI中查找AMRMTokenIdentifier
      appTokenIdentifier = selectAMRMTokenIdentifier(remoteUgi);
      if (appTokenIdentifier == null) {
        tokenFound = false;
        message = "No AMRMToken found for user " + remoteUgi.getUserName();
      } else {
        tokenFound = true;
      }
    } catch (IOException e) {
      tokenFound = false;
      message = "Got exception while looking for AMRMToken for user "
          + remoteUgi.getUserName();
    }

    if (!tokenFound) {
      LOG.warn(message);
      // 未找到合法令牌，抛出认证失败异常
      throw RPCUtil.getRemoteException(message);
    }

    return appTokenIdentifier;
  }

  /**
   * 从远程用户UGI中查找并提取AMRMTokenIdentifier
   * @param remoteUgi 远程请求用户UGI
   * @return 找到的AMRMTokenIdentifier，未找到返回null
   * @throws IOException IO异常
   */
  private static AMRMTokenIdentifier selectAMRMTokenIdentifier(
      UserGroupInformation remoteUgi) throws IOException {
    AMRMTokenIdentifier result = null;
    Set<TokenIdentifier> tokenIds = remoteUgi.getTokenIdentifiers();
    // 遍历所有令牌标识符，找到AMRMToken类型的标识符
    for (TokenIdentifier tokenId : tokenIds) {
      if (tokenId instanceof AMRMTokenIdentifier) {
        result = (AMRMTokenIdentifier) tokenId;
        break;
      }
    }

    return result;
  }

  /**
   * 将RM下发的新AMRMToken更新到RM代理使用的UGI中，替换旧令牌。
   *
   * @param token RM下发的新AMRMToken
   * @param user  RM代理使用的用户UGI
   * @param conf  配置对象
   */
  public static void updateAMRMToken(
      org.apache.hadoop.yarn.api.records.Token token, UserGroupInformation user,
      Configuration conf) {
    // 将YARN API的Token转换为Hadoop安全Token类型
    Token<AMRMTokenIdentifier> amrmToken = new Token<AMRMTokenIdentifier>(
        token.getIdentifier().array(), token.getPassword().array(),
        new Text(token.getKind()), new Text(token.getService()));
    // Preserve the token service sent by the RM when adding the token
    // to ensure we replace the previous token setup by the RM.
    // Afterwards we can update the service address for the RPC layer.
    // 添加新令牌到UGI，会自动替换同服务的旧令牌
    user.addToken(amrmToken);
    // 更新令牌服务地址为本地RM地址，适配RPC层调用
    amrmToken.setService(ClientRMProxy.getAMRMTokenService(conf));
  }

  /**
   * 从容器启动上下文中解析出凭证信息，提取所有安全令牌。
   *
   * @param launchContext 容器启动上下文
   * @return 包含所有令牌的Credentials实例
   * @throws IOException 解析过程IO错误
   */
  public static Credentials parseCredentials(
      ContainerLaunchContext launchContext) throws IOException {
    Credentials credentials = new Credentials();
    ByteBuffer tokens = launchContext.getTokens();

    if (tokens != null) {
      // 准备输入流读取凭证数据
      DataInputByteBuffer buf = new DataInputByteBuffer();
      // 重置缓冲区位置到起始点
      tokens.rewind();
      buf.reset(tokens);
      // 从流中读取凭证存储
      credentials.readTokenStorageStream(buf);
      // 调试模式下日志打印所有令牌信息
      if (LOG.isDebugEnabled()) {
        for (Token<? extends TokenIdentifier> tk : credentials.getAllTokens()) {
          LOG.debug("{}={}", tk.getService(), tk);
        }
      }
    }

    return credentials;
  }
}