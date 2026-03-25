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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocolPB.AliasMapProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.InMemoryAliasMapProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.NameNodeHAProxyFactory;
import org.apache.hadoop.hdfs.server.protocol.BalancerProtocols;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProxyCombiner;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolClientSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;

/**
 * 文件: NameNodeProxies.java
 * 所属模块: HDFS 服务端核心
 * 核心职责: 工厂类，统一创建与远程NameNode通信的RPC代理对象，自动处理HA和非HA场景，所有对远程NameNode的访问都通过该类创建代理
 */
@InterfaceAudience.Private
public class NameNodeProxies {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(NameNodeProxies.class);

  /**
   * 根据配置创建NameNode代理，自动判断是否为HA场景
   * @param conf Hadoop配置对象，包含IPC和故障切换配置
   * @param nameNodeUri NameNode地址URI，可以是具体地址或逻辑命名服务地址
   * @param xface 需要创建的IPC协议接口
   * @return 包含代理对象和对应委派令牌服务信息的包装对象
   * @throws IOException 创建代理失败时抛出异常
   **/
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      URI nameNodeUri, Class<T> xface) throws IOException {
    return createProxy(conf, nameNodeUri, xface, null);
  }

  /**
   * 根据配置创建NameNode代理，自动判断是否为HA场景，支持设置安全回退标识
   * @param conf Hadoop配置对象，包含IPC和故障切换配置
   * @param nameNodeUri NameNode地址URI，可以是具体地址或逻辑命名服务地址
   * @param xface 需要创建的IPC协议接口
   * @param fallbackToSimpleAuth 标识安全客户端是否回退到简单认证
   * @return 包含代理对象和对应委派令牌服务信息的包装对象
   * @throws IOException 创建代理失败时抛出异常
   **/
  @SuppressWarnings("unchecked")
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      URI nameNodeUri, Class<T> xface, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    AbstractNNFailoverProxyProvider<T> failoverProxyProvider =
        NameNodeProxiesClient.createFailoverProxyProvider(conf, nameNodeUri,
            xface, true, fallbackToSimpleAuth, new NameNodeHAProxyFactory<T>());

    if (failoverProxyProvider == null) {
      // 非HA场景，直接创建非HA代理
      return createNonHAProxy(conf, DFSUtilClient.getNNAddress(nameNodeUri),
          xface, UserGroupInformation.getCurrentUser(), true,
          fallbackToSimpleAuth, null);
    } else {
      // HA场景，创建支持故障切换的HA代理
      return NameNodeProxiesClient.createHAProxy(conf, nameNodeUri, xface,
          failoverProxyProvider);
    }
  }

  /**
   * 显式创建非HA场景的NameNode代理，通常不直接使用，应优先调用{@link NameNodeProxies#createProxy}
   * @param conf Hadoop配置对象
   * @param nnAddr 远程NameNode地址
   * @param xface 需要创建的IPC协议接口
   * @param ugi 发起调用的用户信息
   * @param withRetries 是否启用自定义重试策略
   * @return 包含代理对象和对应委派令牌服务信息的包装对象
   * @throws IOException 创建代理失败时抛出异常
   */
  public static <T> ProxyAndInfo<T> createNonHAProxy(
      Configuration conf, InetSocketAddress nnAddr, Class<T> xface,
      UserGroupInformation ugi, boolean withRetries) throws IOException {
    return createNonHAProxy(conf, nnAddr, xface, ugi, withRetries, null, null);
  }

  /**
   * 显式创建非HA场景的NameNode代理，通常不直接使用，应优先调用{@link NameNodeProxies#createProxy}
   * @param conf Hadoop配置对象
   * @param nnAddr 远程NameNode地址
   * @param xface 需要创建的IPC协议接口
   * @param ugi 发起调用的用户信息
   * @param withRetries 是否启用自定义重试策略
   * @param fallbackToSimpleAuth 标识安全客户端是否回退到简单认证
   * @param alignmentContext RPC对齐上下文，用于HA场景的状态对齐
   * @return 包含代理对象和对应委派令牌服务信息的包装对象
   * @throws IOException 创建代理失败时抛出异常
   */
  @SuppressWarnings("unchecked")
  public static <T> ProxyAndInfo<T> createNonHAProxy(
      Configuration conf, InetSocketAddress nnAddr, Class<T> xface,
      UserGroupInformation ugi, boolean withRetries,
      AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
      throws IOException {
    // 构建委派令牌服务标识
    Text dtService = SecurityUtil.buildTokenService(nnAddr);
  
    T proxy;
    // 根据不同协议类型分别创建对应代理
    if (xface == ClientProtocol.class) {
      // 客户端协议，由客户端工具类创建
      proxy = (T) NameNodeProxiesClient.createProxyWithAlignmentContext(
          nnAddr, conf, ugi, withRetries, fallbackToSimpleAuth,
          alignmentContext);
    } else if (xface == JournalProtocol.class) {
      // 日志同步协议，用于QJM共享编辑日志
      proxy = (T) createNNProxyWithJournalProtocol(nnAddr, conf, ugi,
          alignmentContext);
    } else if (xface == NamenodeProtocol.class) {
      // NameNode内部通信协议，用于DataNode和第二NameNode通信
      proxy = (T) createNNProxyWithNamenodeProtocol(nnAddr, conf, ugi,
          withRetries, alignmentContext);
    } else if (xface == GetUserMappingsProtocol.class) {
      // 用户映射获取协议
      proxy = (T) createNNProxyWithGetUserMappingsProtocol(nnAddr, conf, ugi,
          alignmentContext);
    } else if (xface == RefreshUserMappingsProtocol.class) {
      // 刷新用户映射协议
      proxy = (T) createNNProxyWithRefreshUserMappingsProtocol(nnAddr, conf,
          ugi, alignmentContext);
    } else if (xface == RefreshAuthorizationPolicyProtocol.class) {
      // 刷新权限策略协议
      proxy = (T) createNNProxyWithRefreshAuthorizationPolicyProtocol(nnAddr,
          conf, ugi, alignmentContext);
    } else if (xface == RefreshCallQueueProtocol.class) {
      // 刷新调用队列协议
      proxy = (T) createNNProxyWithRefreshCallQueueProtocol(nnAddr, conf, ugi,
          alignmentContext);
    } else if (xface == InMemoryAliasMapProtocol.class) {
      // 内存别名映射协议，用于HDFS路由联邦
      proxy = (T) createNNProxyWithInMemoryAliasMapProtocol(nnAddr, conf, ugi,
          alignmentContext);
    } else if (xface == BalancerProtocols.class) {
      // 负载均衡协议，组合多个接口
      proxy = (T) createNNProxyWithBalancerProtocol(nnAddr, conf, ugi,
          withRetries, fallbackToSimpleAuth, alignmentContext);
    } else {
      // 不支持的协议，抛出异常
      String message = "Unsupported protocol found when creating the proxy " +
          "connection to NameNode: " +
          ((xface != null) ? xface.getClass().getName() : "null");
      LOG.error(message);
      throw new IllegalStateException(message);
    }

    return new ProxyAndInfo<T>(proxy, dtService, nnAddr);
  }

  /**
   * 创建内存别名映射协议的NameNode代理，用于HDFS路由联邦
   * @param address NameNode地址
   * @param conf Hadoop配置
   * @param ugi 用户信息
   * @param alignmentContext RPC对齐上下文
   * @return 内存别名映射协议代理对象
   * @throws IOException 创建失败抛出异常
   */
  private static InMemoryAliasMapProtocol createNNProxyWithInMemoryAliasMapProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    AliasMapProtocolPB proxy = createNameNodeProxy(
        address, conf, ugi, AliasMapProtocolPB.class, 30000, alignmentContext);
    return new InMemoryAliasMapProtocolClientSideTranslatorPB(proxy);
  }

  /**
   * 创建日志同步协议的NameNode代理，用于QJM共享编辑日志
   * @param address NameNode地址
   * @param conf Hadoop配置
   * @param ugi 用户信息
   * @param alignmentContext RPC对齐上下文
   * @return 日志同步协议代理对象
   * @throws IOException 创建失败抛出异常
   */
  private static JournalProtocol createNNProxyWithJournalProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    JournalProtocolPB proxy = createNameNodeProxy(address,
        conf, ugi, JournalProtocolPB.class, 30000, alignmentContext);
    return new JournalProtocolTranslatorPB(proxy);
  }

  /**
   * 创建刷新权限策略协议的NameNode代理
   * @param address NameNode地址
   * @param conf Hadoop配置
   * @param ugi 用户信息
   * @param alignmentContext RPC对齐上下文
   * @return 刷新权限策略协议代理对象
   * @throws IOException 创建失败抛出异常
   */
  private static RefreshAuthorizationPolicyProtocol
      createNNProxyWithRefreshAuthorizationPolicyProtocol(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    RefreshAuthorizationPolicyProtocolPB proxy = createNameNodeProxy(address,
        conf, ugi, RefreshAuthorizationPolicyProtocolPB.class, 0,
        alignmentContext);
    return new RefreshAuthorizationPolicyProtocolClientSideTranslatorPB(proxy);
  }
  
  /**
   * 创建刷新用户映射协议的NameNode代理
   * @param address NameNode地址
   * @param conf Hadoop配置
   * @param ugi 用户信息
   * @param alignmentContext RPC对齐上下文
   * @return 刷新用户映射协议代理对象
   * @throws IOException 创建失败抛出异常
   */
  private static RefreshUserMappingsProtocol
      createNNProxyWithRefreshUserMappingsProtocol(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    RefreshUserMappingsProtocolPB proxy = createNameNodeProxy(address, conf,
        ugi, RefreshUserMappingsProtocolPB.class, 0, alignmentContext);
    return new RefreshUserMappingsProtocolClientSideTranslatorPB(proxy);
  }

  /**
   * 创建刷新调用队列协议的NameNode代理
   * @param address NameNode地址
   * @param conf Hadoop配置
   * @param ugi 用户信息
   * @param alignmentContext RPC对齐上下文
   * @return 刷新调用队列协议代理对象
   * @throws IOException 创建失败抛出异常
   */
  private static RefreshCallQueueProtocol
      createNNProxyWithRefreshCallQueueProtocol(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    RefreshCallQueueProtocolPB proxy = createNameNodeProxy(address, conf, ugi,
        RefreshCallQueueProtocolPB.class, 0, alignmentContext);
    return new RefreshCallQueueProtocolClientSideTranslatorPB(proxy);
  }

  /**
   * 创建获取用户映射协议的NameNode代理
   * @param address NameNode地址
   * @param conf Hadoop配置
   * @param ugi 用户信息
   * @param alignmentContext RPC对齐上下文
   * @return 获取用户映射协议代理对象
   * @throws IOException 创建失败抛出异常
   */
  private static GetUserMappingsProtocol createNNProxyWithGetUserMappingsProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      AlignmentContext alignmentContext) throws IOException {
    GetUserMappingsProtocolPB proxy = createNameNodeProxy(address, conf, ugi,
        GetUserMappingsProtocolPB.class, 0, alignmentContext);
    return new GetUserMappingsProtocolClientSideTranslatorPB(proxy);
  }
  
  /**
   * 创建NameNode内部通信协议的代理，用于DataNode和第二NameNode通信
   * @param address NameNode地址
   * @param conf Hadoop配置
   * @param ugi 用户信息
   * @param withRetries 是否启用自定义重试策略
   * @param alignmentContext RPC对齐上下文
   * @return NameNode内部通信协议代理对象
   * @throws IOException 创建失败抛出异常
   */
  private static NamenodeProtocol createNNProxyWithNamenodeProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      boolean withRetries, AlignmentContext alignmentContext)
      throws IOException {
    NamenodeProtocolPB proxy = createNameNodeProxy(
        address, conf, ugi, NamenodeProtocolPB.class, 0, alignmentContext);
    if (withRetries) { 
      // 需要启用重试，创建带重试策略的代理
      // 指数退避重试策略，最多重试5次，初始间隔200ms
      RetryPolicy timeoutPolicy = RetryPolicies.exponentialBackoffRetry(5, 200,
              TimeUnit.MILLISECONDS);
      // 为特定方法设置重试策略
      Map<String, RetryPolicy> methodNameToPolicyMap
           = new HashMap<String, RetryPolicy>();
      methodNameToPolicyMap.put("getBlocks", timeoutPolicy);
      methodNameToPolicyMap.put("getAccessKeys", timeoutPolicy);
      NamenodeProtocol translatorProxy =
          new NamenodeProtocolTranslatorPB(proxy);
      // 创建Retry代理封装原代理，自动重试特定方法
      return (NamenodeProtocol) RetryProxy.create(
          NamenodeProtocol.class, translatorProxy, methodNameToPolicyMap);
    } else {
      // 不需要重试，直接返回翻译代理
      return new NamenodeProtocolTranslatorPB(proxy);
    }
  }

  /**
   * 创建负载均衡协议代理，组合NamenodeProtocol和ClientProtocol两个接口
   * @param address NameNode地址
   * @param conf Hadoop配置
   * @