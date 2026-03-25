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

package org.apache.hadoop.mapreduce.v2.app.client;

import java.net.InetSocketAddress;

import org.apache.hadoop.service.Service;

/**
 * MapReduce ApplicationMaster客户端服务接口
 * 定义了客户端服务需要提供的基础能力，用于对外暴露服务地址信息
 * 供外部客户端连接访问ApplicationMaster，实现作业提交、状态查询等功能
 */
public interface ClientService extends Service {

  /**
   * 获取客户端服务绑定的网络地址
   * @return 客户端RPC服务绑定的InetSocketAddress地址
   */
  public abstract InetSocketAddress getBindAddress();

  /**
   * 获取客户端服务HTTP端口
   * @return HTTP服务监听端口号
   */
  public abstract int getHttpPort();
}