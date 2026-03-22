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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NameNode高可用代理工厂，用于创建非HA场景下的NameNode代理对象。
 * 实现HAProxyFactory接口，适配NameNodeProxies的非HA代理创建逻辑，
 * 支持在HA架构中为单个NameNode创建通信代理。
 * 
 * @param <T> 代理接口类型
 */
public class NameNodeHAProxyFactory<T> implements HAProxyFactory<T> {

  //  RPC对齐上下文，用于支持状态对齐（如共享编辑日志的主备对齐）
  private AlignmentContext alignmentContext;

  /**
   * 创建NameNode代理对象，支持授权降级标记和状态对齐上下文
   * @param conf Hadoop配置对象
   * @param nnAddr 目标NameNode的网络地址
   * @param xface 代理接口类型
   * @param ugi 代理用户信息
   * @param withRetries 是否开启重试机制
   * @param fallbackToSimpleAuth 是否降级为简单认证，输出参数标记
   * @return 创建完成的代理对象
   * @throws IOException 创建代理失败时抛出异常
   */
  @Override
  public T createProxy(Configuration conf, InetSocketAddress nnAddr,
      Class<T> xface, UserGroupInformation ugi, boolean withRetries,
      AtomicBoolean fallbackToSimpleAuth) throws IOException {
    return NameNodeProxies.createNonHAProxy(conf, nnAddr, xface,
        ugi, withRetries, fallbackToSimpleAuth, alignmentContext).getProxy();
  }

  /**
   * 创建NameNode代理对象，不包含对齐上下文和授权降级标记
   * @param conf Hadoop配置对象
   * @param nnAddr 目标NameNode的网络地址
   * @param xface 代理接口类型
   * @param ugi 代理用户信息
   * @param withRetries 是否开启重试机制
   * @return 创建完成的代理对象
   * @throws IOException 创建代理失败时抛出异常
   */
  @Override
  public T createProxy(Configuration conf, InetSocketAddress nnAddr,
      Class<T> xface, UserGroupInformation ugi, boolean withRetries)
      throws IOException {
    return NameNodeProxies.createNonHAProxy(conf, nnAddr, xface,
      ugi, withRetries).getProxy();
  }

  /**
   * 设置RPC对齐上下文，用于主备NameNode状态对齐
   * @param alignmentContext 对齐上下文实例
   */
  public void setAlignmentContext(AlignmentContext alignmentContext) {
    this.alignmentContext = alignmentContext;
  }
}