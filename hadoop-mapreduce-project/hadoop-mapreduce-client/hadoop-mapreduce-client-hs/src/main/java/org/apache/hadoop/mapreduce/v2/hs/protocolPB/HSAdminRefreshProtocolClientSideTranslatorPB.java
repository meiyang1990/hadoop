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

package org.apache.hadoop.mapreduce.v2.hs.protocolPB;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocolPB;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsRequestProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * 历史服务器管理刷新协议的客户端PB翻译器，负责将本地接口调用转换为基于Protobuf的RPC调用
 * 实现了历史服务器管理刷新协议的客户端侧编解码转换，对接Protobuf序列化格式的RPC服务
 */
@Private
public class HSAdminRefreshProtocolClientSideTranslatorPB implements
    ProtocolMetaInterface, HSAdminRefreshProtocol, Closeable {

  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;

  private final HSAdminRefreshProtocolPB rpcProxy;
  // 空请求对象，复用避免重复创建
  private final static RefreshAdminAclsRequestProto 
    VOID_REFRESH_ADMIN_ACLS_REQUEST = RefreshAdminAclsRequestProto
      .newBuilder().build();
  // 空请求对象，复用避免重复创建
  private final static RefreshLoadedJobCacheRequestProto 
    VOID_REFRESH_LOADED_JOB_CACHE_REQUEST = RefreshLoadedJobCacheRequestProto
      .newBuilder().build();
  // 空请求对象，复用避免重复创建
  private final static RefreshJobRetentionSettingsRequestProto 
    VOID_REFRESH_JOB_RETENTION_SETTINGS_REQUEST = 
       RefreshJobRetentionSettingsRequestProto.newBuilder().build();
  // 空请求对象，复用避免重复创建
  private final static RefreshLogRetentionSettingsRequestProto 
    VOID_REFRESH_LOG_RETENTION_SETTINGS_REQUEST = 
      RefreshLogRetentionSettingsRequestProto.newBuilder().build();

  /**
   * 构造函数，使用已创建的Protobuf RPC代理初始化翻译器
   * @param rpcProxy Protobuf格式的历史服务器刷新协议RPC代理
   */
  public HSAdminRefreshProtocolClientSideTranslatorPB(
      HSAdminRefreshProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  /**
   * 关闭RPC代理，释放连接资源
   * @throws IOException 关闭过程中发生IO异常
   */
  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  /**
   * 刷新历史服务器管理员ACL权限配置
   * @throws IOException RPC调用过程中发生IO异常
   */
  @Override
  public void refreshAdminAcls() throws IOException {
    ipc(() -> rpcProxy.refreshAdminAcls(NULL_CONTROLLER,
        VOID_REFRESH_ADMIN_ACLS_REQUEST));
  }


  /**
   * 刷新已加载作业缓存，重新加载作业信息
   * @throws IOException RPC调用过程中发生IO异常
   */
  @Override
  public void refreshLoadedJobCache() throws IOException {
    ipc(() -> rpcProxy.refreshLoadedJobCache(NULL_CONTROLLER,
        VOID_REFRESH_LOADED_JOB_CACHE_REQUEST));
  }
  
  /**
   * 刷新作业保留时间配置，应用新的作业清理策略
   * @throws IOException RPC调用过程中发生IO异常
   */
  @Override
  public void refreshJobRetentionSettings() throws IOException {
    ipc(() -> rpcProxy.refreshJobRetentionSettings(NULL_CONTROLLER,
        VOID_REFRESH_JOB_RETENTION_SETTINGS_REQUEST));
  }

  /**
   * 刷新日志保留时间配置，应用新的日志清理策略
   * @throws IOException RPC调用过程中发生IO异常
   */
  @Override
  public void refreshLogRetentionSettings() throws IOException {
    ipc(() -> rpcProxy.refreshLogRetentionSettings(NULL_CONTROLLER,
        VOID_REFRESH_LOG_RETENTION_SETTINGS_REQUEST));
  }

  /**
   * 检查远程服务端是否支持指定方法
   * @param methodName 待检查的方法名称
   * @return 如果服务端支持该方法返回true，否则返回false
   * @throws IOException 查询过程中发生IO异常
   */
  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        HSAdminRefreshProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(HSAdminRefreshProtocolPB.class), methodName);
  }

}