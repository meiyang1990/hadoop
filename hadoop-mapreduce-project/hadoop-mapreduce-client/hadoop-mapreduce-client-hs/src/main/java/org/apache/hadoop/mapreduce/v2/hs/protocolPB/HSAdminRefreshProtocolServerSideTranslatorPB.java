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

/**
 * @file HSAdminRefreshProtocolServerSideTranslatorPB.java
 * 历史服务器管理刷新协议Protobuf服务端转换器，负责将PB格式的RPC请求转换为内部接口调用
 */
package org.apache.hadoop.mapreduce.v2.hs.protocolPB;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocolPB;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshAdminAclsResponseProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheResponseProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsResponseProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsResponseProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * 历史服务器管理刷新协议PB服务端转换器
 * 负责处理Protobuf序列化的RPC请求，将请求转发给内部历史服务器刷新接口实现，
 * 并将结果转换为Protobuf格式返回给客户端，完成协议格式转换
 */
@Private
public class HSAdminRefreshProtocolServerSideTranslatorPB implements
    HSAdminRefreshProtocolPB {

  // 内部历史服务器刷新协议实现实例
  private final HSAdminRefreshProtocol impl;

  // 空刷新管理员ACL响应单例，该操作无返回数据
  private final static RefreshAdminAclsResponseProto 
    VOID_REFRESH_ADMIN_ACLS_RESPONSE = RefreshAdminAclsResponseProto
      .newBuilder().build();

  // 空刷新已加载作业缓存响应单例，该操作无返回数据
  private final static RefreshLoadedJobCacheResponseProto 
    VOID_REFRESH_LOADED_JOB_CACHE_RESPONSE = RefreshLoadedJobCacheResponseProto
      .newBuilder().build();

  // 空刷新作业保留策略设置响应单例，该操作无返回数据
  private final static RefreshJobRetentionSettingsResponseProto 
    VOID_REFRESH_JOB_RETENTION_SETTINGS_RESPONSE = 
      RefreshJobRetentionSettingsResponseProto.newBuilder().build();

  // 空刷新日志保留策略设置响应单例，该操作无返回数据
  private final static RefreshLogRetentionSettingsResponseProto 
    VOID_REFRESH_LOG_RETENTION_SETTINGS_RESPONSE = 
      RefreshLogRetentionSettingsResponseProto.newBuilder().build();

  /**
   * 构造转换器，绑定内部刷新协议实现
   * @param impl 历史服务器刷新协议内部实现实例
   */
  public HSAdminRefreshProtocolServerSideTranslatorPB(
      HSAdminRefreshProtocol impl) {
    this.impl = impl;
  }

  /**
   * 处理刷新管理员ACL的PB RPC请求
   * @param controller RPC控制器
   * @param request PB格式请求
   * @return PB格式空响应
   * @throws ServiceException 服务异常，包装内部IO异常
   */
  @Override
  public RefreshAdminAclsResponseProto refreshAdminAcls(
      RpcController controller, RefreshAdminAclsRequestProto request)
      throws ServiceException {
    try {
      impl.refreshAdminAcls();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_ADMIN_ACLS_RESPONSE;
  }

  /**
   * 处理刷新已加载作业缓存的PB RPC请求
   * @param controller RPC控制器
   * @param request PB格式请求
   * @return PB格式空响应
   * @throws ServiceException 服务异常，包装内部IO异常
   */
  @Override
  public RefreshLoadedJobCacheResponseProto refreshLoadedJobCache(
      RpcController controller, RefreshLoadedJobCacheRequestProto request)
      throws ServiceException {
    try {
      impl.refreshLoadedJobCache();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_LOADED_JOB_CACHE_RESPONSE;
  }

  /**
   * 处理刷新作业保留策略设置的PB RPC请求
   * @param controller RPC控制器
   * @param request PB格式请求
   * @return PB格式空响应
   * @throws ServiceException 服务异常，包装内部IO异常
   */
  @Override
  public RefreshJobRetentionSettingsResponseProto refreshJobRetentionSettings(
      RpcController controller, 
      RefreshJobRetentionSettingsRequestProto request)
      throws ServiceException {
    try {
      impl.refreshJobRetentionSettings();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_JOB_RETENTION_SETTINGS_RESPONSE;
  }

  /**
   * 处理刷新日志保留策略设置的PB RPC请求
   * @param controller RPC控制器
   * @param request PB格式请求
   * @return PB格式空响应
   * @throws ServiceException 服务异常，包装内部IO异常
   */
  @Override
  public RefreshLogRetentionSettingsResponseProto refreshLogRetentionSettings(
      RpcController controller, 
      RefreshLogRetentionSettingsRequestProto request)
      throws ServiceException {
    try {
      impl.refreshLogRetentionSettings();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_LOG_RETENTION_SETTINGS_RESPONSE;
  }
}