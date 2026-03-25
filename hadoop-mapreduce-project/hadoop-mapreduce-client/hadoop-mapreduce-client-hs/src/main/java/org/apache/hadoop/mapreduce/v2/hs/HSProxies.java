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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocolPB;
import org.apache.hadoop.mapreduce.v2.hs.protocolPB.HSAdminRefreshProtocolClientSideTranslatorPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 历史服务器代理工厂类，负责为不同管理协议创建连接历史服务器的RPC代理
 * 为客户端访问历史服务器管理接口提供统一的代理创建入口
 */
@Private
public class HSProxies {

  private static final Logger LOG = LoggerFactory.getLogger(HSProxies.class);

  /**
   * 根据指定协议类型创建连接历史服务器的RPC代理
   * @param conf 配置对象
   * @param hsaddr 历史服务器服务地址
   * @param xface 协议接口类型
   * @param ugi 当前用户凭证信息
   * @return 创建好的RPC代理对象
   * @throws IOException 创建代理失败时抛出IO异常
   * @throws IllegalStateException 不支持的协议类型时抛出异常
   */
  @SuppressWarnings("unchecked")
  public static <T> T createProxy(Configuration conf, InetSocketAddress hsaddr,
      Class<T> xface, UserGroupInformation ugi) throws IOException {

    T proxy;
    // 根据请求协议类型分发到对应创建方法
    if (xface == RefreshUserMappingsProtocol.class) {
      proxy = (T) createHSProxyWithRefreshUserMappingsProtocol(hsaddr, conf,
          ugi);
    } else if (xface == GetUserMappingsProtocol.class) {
      proxy = (T) createHSProxyWithGetUserMappingsProtocol(hsaddr, conf, ugi);
    } else if (xface == HSAdminRefreshProtocol.class) {
      proxy = (T) createHSProxyWithHSAdminRefreshProtocol(hsaddr, conf, ugi);
    } else {
      // 不支持的协议类型，记录错误并抛出异常
      String message = "Unsupported protocol found when creating the proxy "
          + "connection to History server: "
          + ((xface != null) ? xface.getClass().getName() : "null");
      LOG.error(message);
      throw new IllegalStateException(message);
    }
    return proxy;
  }

  /**
   * 创建用户映射刷新协议的历史服务器代理
   * @param address 历史服务器地址
   * @param conf 配置对象
   * @param ugi 当前用户凭证
   * @return 用户映射刷新协议代理对象
   * @throws IOException 创建失败抛出IO异常
   */
  private static RefreshUserMappingsProtocol createHSProxyWithRefreshUserMappingsProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi)
      throws IOException {
    RefreshUserMappingsProtocolPB proxy = (RefreshUserMappingsProtocolPB) createHSProxy(
        address, conf, ugi, RefreshUserMappingsProtocolPB.class, 0);
    return new RefreshUserMappingsProtocolClientSideTranslatorPB(proxy);
  }

  /**
   * 创建用户信息获取协议的历史服务器代理
   * @param address 历史服务器地址
   * @param conf 配置对象
   * @param ugi 当前用户凭证
   * @return 用户信息获取协议代理对象
   * @throws IOException 创建失败抛出IO异常
   */
  private static GetUserMappingsProtocol createHSProxyWithGetUserMappingsProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi)
      throws IOException {
    GetUserMappingsProtocolPB proxy = (GetUserMappingsProtocolPB) createHSProxy(
        address, conf, ugi, GetUserMappingsProtocolPB.class, 0);
    return new GetUserMappingsProtocolClientSideTranslatorPB(proxy);
  }

  /**
   * 创建历史服务器管理刷新协议的代理
   * @param hsaddr 历史服务器地址
   * @param conf 配置对象
   * @param ugi 当前用户凭证
   * @return 历史服务器管理刷新协议代理对象
   * @throws IOException 创建失败抛出IO异常
   */
  private static HSAdminRefreshProtocol createHSProxyWithHSAdminRefreshProtocol(
      InetSocketAddress hsaddr, Configuration conf, UserGroupInformation ugi)
      throws IOException {
    HSAdminRefreshProtocolPB proxy = (HSAdminRefreshProtocolPB) createHSProxy(
        hsaddr, conf, ugi, HSAdminRefreshProtocolPB.class, 0);
    return new HSAdminRefreshProtocolClientSideTranslatorPB(proxy);
  }

  /**
   * 底层通用RPC代理创建方法，负责初始化RPC引擎并获取代理
   * @param address 历史服务器地址
   * @param conf 配置对象
   * @param ugi 当前用户凭证
   * @param xface 协议接口类型
   * @param rpcTimeout RPC超时时间
   * @return 创建好的底层RPC代理对象
   * @throws IOException 创建失败抛出IO异常
   */
  private static Object createHSProxy(InetSocketAddress address,
      Configuration conf, UserGroupInformation ugi, Class<?> xface,
      int rpcTimeout) throws IOException {
    // 设置协议引擎为Protobuf序列化RPC引擎
    RPC.setProtocolEngine(conf, xface, ProtobufRpcEngine2.class);
    // 获取RPC代理对象
    Object proxy = RPC.getProxy(xface, RPC.getProtocolVersion(xface), address,
        ugi, conf, NetUtils.getDefaultSocketFactory(conf), rpcTimeout);
    return proxy;
  }
}