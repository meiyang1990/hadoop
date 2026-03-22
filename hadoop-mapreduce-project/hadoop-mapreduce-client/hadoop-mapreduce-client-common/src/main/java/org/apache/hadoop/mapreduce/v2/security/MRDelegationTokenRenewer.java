// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.mapreduce.v2.security;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce 委托令牌续订器，负责处理 MapReduce 委托令牌的续订、取消操作
 * 用于在长时间运行的 MapReduce 作业中自动维护令牌有效性，保证与历史服务器等服务的身份认证正常
 */
@InterfaceAudience.Private
public class MRDelegationTokenRenewer extends TokenRenewer {

  private static final Logger LOG = LoggerFactory
      .getLogger(MRDelegationTokenRenewer.class);

  /**
   * 判断当前 renewer 是否能够处理指定类型的令牌
   * @param kind 令牌类型标识
   * @return 如果是 MapReduce 委托令牌则返回 true，否则返回 false
   */
  @Override
  public boolean handleKind(Text kind) {
    return MRDelegationTokenIdentifier.KIND_NAME.equals(kind);
  }

  /**
   * 向 MapReduce 历史服务器续订委托令牌，更新过期时间
   * @param token 需要续订的委托令牌
   * @param conf Hadoop 配置对象
   * @return 续订后新的过期时间戳
   * @throws IOException 网络或RPC调用异常
   * @throws InterruptedException 线程中断异常
   */
  @Override
  public long renew(Token<?> token, Configuration conf) throws IOException,
      InterruptedException {
    // 将Hadoop通用Token对象转换为Yarn Token格式
    org.apache.hadoop.yarn.api.records.Token dToken =
        org.apache.hadoop.yarn.api.records.Token.newInstance(
          token.getIdentifier(), token.getKind().toString(),
          token.getPassword(), token.getService().toString());

    // 创建到历史服务器的RPC代理
    MRClientProtocol histProxy = instantiateHistoryProxy(conf,
        SecurityUtil.getTokenServiceAddr(token));
    try {
      // 构造续订请求并发送
      RenewDelegationTokenRequest request = Records
          .newRecord(RenewDelegationTokenRequest.class);
      request.setDelegationToken(dToken);
      return histProxy.renewDelegationToken(request).getNextExpirationTime();
    } finally {
      // 关闭RPC代理释放资源
      stopHistoryProxy(histProxy);
    }

  }

  /**
   * 取消指定的 MapReduce 委托令牌，使其提前失效
   * @param token 需要取消的委托令牌
   * @param conf Hadoop 配置对象
   * @throws IOException 网络或RPC调用异常
   * @throws InterruptedException 线程中断异常
   */
  @Override
  public void cancel(Token<?> token, Configuration conf) throws IOException,
      InterruptedException {
    // 将Hadoop通用Token对象转换为Yarn Token格式
    org.apache.hadoop.yarn.api.records.Token dToken =
        org.apache.hadoop.yarn.api.records.Token.newInstance(
          token.getIdentifier(), token.getKind().toString(),
          token.getPassword(), token.getService().toString());

    // 创建到历史服务器的RPC代理
    MRClientProtocol histProxy = instantiateHistoryProxy(conf,
        SecurityUtil.getTokenServiceAddr(token));
    try {
      // 构造取消请求并发送
      CancelDelegationTokenRequest request = Records
          .newRecord(CancelDelegationTokenRequest.class);
      request.setDelegationToken(dToken);
      histProxy.cancelDelegationToken(request);
    } finally {
      // 关闭RPC代理释放资源
      stopHistoryProxy(histProxy);
    }
  }

  /**
   * 指示当前令牌是否由系统框架自动管理续订
   * @return 始终返回 true，表示 MR 委托令牌由本 renewer 自动管理续订
   * @throws IOException IO异常
   */
  @Override
  public boolean isManaged(Token<?> token) throws IOException {
    return true;
  }

  /**
   * 关闭历史服务器RPC代理，释放连接资源
   * @param proxy 需要关闭的RPC代理对象
   */
  protected void stopHistoryProxy(MRClientProtocol proxy) {
    RPC.stopProxy(proxy);
  }

  /**
   * 创建到 MapReduce 历史服务器的RPC代理客户端，用于令牌操作请求
   * @param conf Hadoop 配置对象
   * @param hsAddress 历史服务器服务地址
   * @return 历史服务器RPC代理实例
   * @throws IOException 创建代理失败时抛出IO异常
   */
  protected MRClientProtocol instantiateHistoryProxy(final Configuration conf,
      final InetSocketAddress hsAddress) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to MRHistoryServer at: " + hsAddress);
    }
    // 创建Yarn RPC实例
    final YarnRPC rpc = YarnRPC.create(conf);
    // 获取当前调用用户上下文
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    // 在当前用户权限上下文中创建RPC代理
    return currentUser.doAs(new PrivilegedAction<MRClientProtocol>() {
      @Override
      public MRClientProtocol run() {
        return (MRClientProtocol) rpc.getProxy(HSClientProtocol.class,
            hsAddress, conf);
      }
    });
  }
}