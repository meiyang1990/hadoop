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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol;

/**
 * 定义了AMRMProxy请求拦截器需要实现的契约，拦截器用于拦截检查从Application Master发送到Resource Manager的消息
 */
public interface RequestInterceptor extends DistributedSchedulingAMProtocol,
    Configurable {
  /**
   * 初始化拦截器，该方法在实例生命周期中保证只调用一次
   *
   * @param ctx AMRMProxy应用上下文
   */
  void init(AMRMProxyApplicationContext ctx);

  /**
   * 当NM恢复启用时，恢复拦截器状态。AMRMProxy会将恢复数据存入AMRMProxyApplicationContext的恢复数据字典，
   * 所有拦截器需要从中恢复自身状态。
   * 例如：注册请求需要被链尾拦截器（实际连接RM的拦截器）保存，以便在RM故障转移时重新注册
   *
   * @param recoveredDataMap 从NM状态存储中恢复的所有拦截器状态数据
   */
  void recover(Map<String, byte[]> recoveredDataMap);

  /**
   * 释放拦截器持有的资源，在应用销毁管道时调用。具体实现需要释放资源，并将请求转发给下一个拦截器（如果存在）
   */
  void shutdown();

  /**
   * 设置管道中的下一个拦截器。该接口的具体实现需要在检查消息后将请求传递给下一个拦截器。
   * 链中最后一个拦截器负责发送消息到Resource Manager服务，因此最后一个拦截器不会收到该调用
   *
   * @param nextInterceptor 要设置的下一个拦截器
   */
  void setNextInterceptor(RequestInterceptor nextInterceptor);

  /**
   * 获取责任链中的下一个拦截器
   * 
   * @return 责任链中的下一个拦截器
   */
  RequestInterceptor getNextInterceptor();

  /**
   * 获取当前拦截器对应的应用上下文
   * 
   * @return 应用上下文
   */
  AMRMProxyApplicationContext getApplicationContext();
}