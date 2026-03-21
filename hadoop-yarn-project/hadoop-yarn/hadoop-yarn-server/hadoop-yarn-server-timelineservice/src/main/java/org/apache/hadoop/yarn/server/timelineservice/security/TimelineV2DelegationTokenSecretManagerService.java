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

package org.apache.hadoop.yarn.server.timelineservice.security;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.timeline.security.TimelineDelgationTokenSecretManagerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timeline V2 时间线服务的委托令牌密钥管理器服务包装类，为 ATSv2 提供委托令牌管理的服务封装。
 */
public class TimelineV2DelegationTokenSecretManagerService extends
    TimelineDelgationTokenSecretManagerService {

  /**
   * 构造 Timeline V2 委托令牌密钥管理器服务实例。
   */
  public TimelineV2DelegationTokenSecretManagerService() {
    super(TimelineV2DelegationTokenSecretManagerService.class.getName());
  }

  /**
   * 创建 Timeline V2 版本的委托令牌密钥管理器实例。
   * @param secretKeyInterval 密钥滚动更新间隔（毫秒）
   * @param tokenMaxLifetime 委托令牌最大生命周期（毫秒）
   * @param tokenRenewInterval 委托令牌必须更新的间隔（毫秒）
   * @param tokenRemovalScanInterval 扫描过期令牌的间隔（毫秒）
   * @return 初始化完成的 Timeline V2 委托令牌密钥管理器
   */
  @Override
  protected AbstractDelegationTokenSecretManager
      <TimelineDelegationTokenIdentifier>
      createTimelineDelegationTokenSecretManager(long secretKeyInterval,
          long tokenMaxLifetime, long tokenRenewInterval,
          long tokenRemovalScanInterval) {
    return new TimelineV2DelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, tokenRemovalScanInterval);
  }

  /**
   * 为指定用户生成新的 Timeline V2 委托令牌。
   * @param ugi 用户信息对象
   * @param renewer 允许更新此令牌的用户
   * @return 生成完成的委托令牌
   */
  public Token<TimelineDelegationTokenIdentifier> generateToken(
      UserGroupInformation ugi, String renewer) {
    return ((TimelineV2DelegationTokenSecretManager)
        getTimelineDelegationTokenSecretManager()).generateToken(ugi, renewer);
  }

  /**
   * 更新指定 Timeline 委托令牌，延长其有效期。
   * @param token 待更新的委托令牌
   * @param renewer 请求更新的用户名
   * @return 更新后令牌的过期时间戳
   * @throws IOException 更新过程中发生 I/O 错误
   */
  public long renewToken(Token<TimelineDelegationTokenIdentifier> token,
      String renewer) throws IOException {
    return getTimelineDelegationTokenSecretManager().renewToken(token, renewer);
  }

  /**
   * 取消指定 Timeline 委托令牌，使其立即失效。
   * @param token 待取消的委托令牌
   * @param canceller 请求取消的用户名
   * @throws IOException 取消过程中发生 I/O 错误
   */
  public void cancelToken(Token<TimelineDelegationTokenIdentifier> token,
      String canceller) throws IOException {
    getTimelineDelegationTokenSecretManager().cancelToken(token, canceller);
  }

  /**
   * Timeline V2 时间线服务专用的委托令牌密钥管理器，负责 ATSv2 委托令牌的生成、更新、过期管理。
   */
  @Private
  @Unstable
  public static class TimelineV2DelegationTokenSecretManager extends
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier> {

    private static final Logger LOG =
        LoggerFactory.getLogger(TimelineV2DelegationTokenSecretManager.class);

    /**
     * 创建 Timeline V2 委托令牌密钥管理器实例。
     * @param delegationKeyUpdateInterval 密钥滚动更新间隔（毫秒）
     * @param delegationTokenMaxLifetime 委托令牌最大生命周期（毫秒）
     * @param delegationTokenRenewInterval 委托令牌必须更新的间隔（毫秒）
     * @param delegationTokenRemoverScanInterval 扫描过期令牌的间隔（毫秒）
     */
    public TimelineV2DelegationTokenSecretManager(
        long delegationKeyUpdateInterval, long delegationTokenMaxLifetime,
        long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    }

    /**
     * 为指定用户生成 Timeline V2 委托令牌。
     * @param ugi 用户信息对象，包含当前用户以及代理真实用户信息
     * @param renewer 允许更新此令牌的用户
     * @return 生成完成的委托令牌实例
     */
    public Token<TimelineDelegationTokenIdentifier> generateToken(
        UserGroupInformation ugi, String renewer) {
      // 保存代理的真实用户信息（如果存在）
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }
      // 创建令牌标识符并填充权限信息
      TimelineDelegationTokenIdentifier identifier = createIdentifier();
      identifier.setOwner(new Text(ugi.getUserName()));
      identifier.setRenewer(new Text(renewer));
      identifier.setRealUser(realUser);
      // 根据标识符生成密钥密码，组装为完整令牌返回
      byte[] password = createPassword(identifier);
      return new Token<TimelineDelegationTokenIdentifier>(identifier.getBytes(),
          password, identifier.getKind(), null);
    }

    @Override
    public TimelineDelegationTokenIdentifier createIdentifier() {
      return new TimelineDelegationTokenIdentifier();
    }

    /**
     * 令牌过期时的日志记录，打印过期令牌信息。
     * @param ident 过期的令牌标识符
     * @throws IOException 记录过程中发生 I/O 错误
     */
    @Override
    protected void logExpireToken(TimelineDelegationTokenIdentifier ident)
        throws IOException {
      LOG.info("Token " + ident + " expired.");
    }
  }
}