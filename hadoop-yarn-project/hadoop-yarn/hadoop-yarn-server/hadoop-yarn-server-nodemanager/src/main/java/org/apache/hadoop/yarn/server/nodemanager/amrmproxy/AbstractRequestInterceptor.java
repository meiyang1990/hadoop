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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;

/**
 * 文件说明：AMRMProxy请求拦截器抽象基类，实现了RequestInterceptor接口，
 * 提供责任链模式的基础基础设施，具体拦截器可继承此类扩展自定义处理逻辑。
 * 核心职责：维护责任链引用，提供通用生命周期方法转发，为分布式调度请求提供默认实现。
 */
public abstract class AbstractRequestInterceptor implements
    RequestInterceptor {
  private Configuration conf;
  private AMRMProxyApplicationContext appContext;
  private RequestInterceptor nextInterceptor;

  /**
   * 设置责任链中的下一个拦截器
   */
  @Override
  public void setNextInterceptor(RequestInterceptor nextInterceptor) {
    this.nextInterceptor = nextInterceptor;
  }

  /**
   * 设置配置对象，同时转发给下一个拦截器
   */

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (this.nextInterceptor != null) {
      this.nextInterceptor.setConf(conf);
    }
  }

  /**
   * 获取当前配置对象
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * 初始化拦截器，同时转发初始化给下一个拦截器
   */
  @Override
  public void init(AMRMProxyApplicationContext appContext) {
    // 检查拦截器是否重复初始化，保证单次初始化
    Preconditions.checkState(this.appContext == null,
        "init is called multiple times on this interceptor: "
            + this.getClass().getName());
    this.appContext = appContext;
    if (this.nextInterceptor != null) {
      this.nextInterceptor.init(appContext);
    }
  }

  /**
   * 从状态存储恢复拦截器状态，同时转发恢复操作给下一个拦截器
   */
  @Override
  public void recover(Map<String, byte[]> recoveredDataMap) {
    if (this.nextInterceptor != null) {
      this.nextInterceptor.recover(recoveredDataMap);
    }
  }

  /**
   * 关闭拦截器，同时转发关闭操作给下一个拦截器
   */
  @Override
  public void shutdown() {
    if (this.nextInterceptor != null) {
      this.nextInterceptor.shutdown();
    }
  }

  /**
   * 获取责任链中的下一个拦截器
   */
  @Override
  public RequestInterceptor getNextInterceptor() {
    return this.nextInterceptor;
  }

  /**
   * 获取当前AMRMProxy应用上下文
   */
  public AMRMProxyApplicationContext getApplicationContext() {
    return this.appContext;
  }

  /**
   * 分布式调度分配请求默认处理实现，转发给责任链下一个拦截器处理
   *
   * @param request 分布式调度分配请求
   * @return 分布式调度分配响应
   * @throws YarnException  yarn处理异常
   * @throws IOException IO异常
   */
  @Override
  public DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
      DistributedSchedulingAllocateRequest request)
      throws YarnException, IOException {
    return (this.nextInterceptor != null) ?
        this.nextInterceptor.allocateForDistributedScheduling(request) : null;
  }

  /**
   * 分布式调度AM注册请求默认处理实现，转发给责任链下一个拦截器处理
   *
   * @param request AM注册请求
   * @return 分布式调度AM注册响应
   * @throws YarnException yarn处理异常
   * @throws IOException IO异常
   */
  @Override
  public RegisterDistributedSchedulingAMResponse
      registerApplicationMasterForDistributedScheduling(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    return (this.nextInterceptor != null) ? this.nextInterceptor
        .registerApplicationMasterForDistributedScheduling(request) : null;
  }

  /**
   * 获取NodeManager状态存储服务实例
   *
   * @return NM状态存储实例，上下文不存在时返回null
   */
  public NMStateStoreService getNMStateStore() {
    if (this.appContext == null || this.appContext.getNMContext() == null) {
      return null;
    }
    return this.appContext.getNMContext().getNMStateStore();
  }
}