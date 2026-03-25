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
package org.apache.hadoop.yarn.server.router.rmadmin;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationMethodWrapper;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * RM管理协议方法封装类，保存管理方法的参数信息，支持向多个/单个子集群并发执行管理命令。
 * 继承FederationMethodWrapper，提供联邦环境下的RM管理操作反射调用能力。
 */
public class RMAdminProtocolMethod extends FederationMethodWrapper {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMAdminProtocolMethod.class);

  // 联邦状态存储门面，用于获取子集群信息
  private FederationStateStoreFacade federationFacade;
  // RM管理拦截器，用于获取RM代理和线程池
  private FederationRMAdminInterceptor rmAdminInterceptor;
  // 配置对象
  private Configuration configuration;

  /**
   * 构造方法，初始化方法参数类型和实际参数。
   * @param pTypes 方法参数类型数组
   * @param pParams 方法实际参数数组
   * @throws IOException 构造异常
   */
  public RMAdminProtocolMethod(Class<?>[] pTypes, Object... pParams)
      throws IOException {
    super(pTypes, pParams);
  }

  /**
   * 根据是否指定子集群ID，选择并发调用所有活跃子集群或单调用指定子集群。
   * @param interceptor RM管理拦截器
   * @param clazz 返回结果类型
   * @param subClusterId 目标子集群ID，为空则调用所有子集群
   * @param <R> 返回结果泛型
   * @return 聚合后的结果集合
   * @throws YarnException 调用异常
   */
  public <R> Collection<R> invokeConcurrent(FederationRMAdminInterceptor interceptor,
      Class<R> clazz, String subClusterId) throws YarnException {
    this.rmAdminInterceptor = interceptor;
    this.federationFacade = FederationStateStoreFacade.getInstance(interceptor.getConf());
    this.configuration = interceptor.getConf();
    if (StringUtils.isNotBlank(subClusterId)) {
      // 指定子集群，单集群调用
      return invoke(clazz, subClusterId);
    } else {
      // 未指定，并发调用所有活跃子集群
      return invokeConcurrent(clazz);
    }
  }

  @Override
  /**
   * 并发调用所有活跃子集群执行当前RM管理方法。
   * @param clazz 返回结果类型
   * @param <R> 返回结果泛型
   * @return 所有子集群返回的结果集合
   * @throws YarnException 任何子集群调用失败则抛出异常
   */
  protected <R> Collection<R> invokeConcurrent(Class<R> clazz) throws YarnException {
    // 从调用栈获取要执行的方法名称
    String methodName = Thread.currentThread().getStackTrace()[3].getMethodName();
    this.setMethodName(methodName);

    // 获取RM管理拦截器中的并发线程池
    ThreadPoolExecutor executorService = rmAdminInterceptor.getExecutorService();

    // 获取所有活跃子集群信息
    Map<SubClusterId, SubClusterInfo> subClusterInfo =
        federationFacade.getSubClusters(true);
    Collection<SubClusterId> subClusterIds = subClusterInfo.keySet();

    // 初始化任务集合、结果未来对象集合、异常集合
    List<Callable<Pair<SubClusterId, Object>>> callables = new ArrayList<>();
    List<Future<Pair<SubClusterId, Object>>> futures = new ArrayList<>();
    Map<SubClusterId, Exception> exceptions = new TreeMap<>();

    // 为每个子集群生成并行调用任务
    for (SubClusterId subClusterId : subClusterIds) {
      callables.add(() -> {
        // 获取对应子集群的RM管理协议代理
        ResourceManagerAdministrationProtocol protocol =
            rmAdminInterceptor.getAdminRMProxyForSubCluster(subClusterId);
        Class<?>[] types = this.getTypes();
        Object[] params = this.getParams();
        // 反射获取目标方法
        Method method = ResourceManagerAdministrationProtocol.class.getMethod(methodName, types);
        // 反射调用方法
        Object result = method.invoke(protocol, params);
        return Pair.of(subClusterId, result);
      });
    }

    // 结果存储map
    Map<SubClusterId, R> results = new TreeMap<>();
    try {
      // 并发执行所有任务，等待全部完成
      futures.addAll(executorService.invokeAll(callables));
      // 遍历处理所有任务结果
      futures.stream().forEach(future -> {
        SubClusterId subClusterId = null;
        try {
          // 获取任务执行结果
          Pair<SubClusterId, Object> pair = future.get();
          subClusterId = pair.getKey();
          Object result = pair.getValue();
          if (result != null) {
            // 类型转换后存入结果map
            R rResult = clazz.cast(result);
            results.put(subClusterId, rResult);
          }
        } catch (InterruptedException | ExecutionException e) {
          // 记录执行异常
          Throwable cause = e.getCause();
          LOG.error("Cannot execute {} on {}: {}", methodName, subClusterId, cause.getMessage());
          exceptions.put(subClusterId, e);
        }
      });
    } catch (InterruptedException e) {
      // 整体执行被中断，抛出异常
      throw new YarnException("invokeConcurrent Failed.", e);
    }

    // 只要有任意子集群执行异常，整体抛出异常
    if (exceptions != null && !exceptions.isEmpty()) {
      Set<SubClusterId> subClusterIdSets = exceptions.keySet();
      throw new YarnException("invokeConcurrent Failed, An exception occurred in subClusterIds = " +
          StringUtils.join(subClusterIdSets, ","));
    }

    // 返回所有子集群的执行结果
    return results.values();
  }

  /**
   * 在指定单个子集群上调用目标RM管理方法。
   *
   * @param clazz 返回类型
   * @param subClusterId 目标子集群 Id
   * @param <R> 泛型返回类型
   * @return 单元素结果集合
   * @throws YarnException 子集群不存在/不活跃或调用异常抛出
   */
  protected <R> Collection<R> invoke(Class<R> clazz, String subClusterId) throws YarnException {

    // 从调用栈获取要执行的方法名称
    String methodName = Thread.currentThread().getStackTrace()[3].getMethodName();
    this.setMethodName(methodName);

    // 获取所有活跃子集群信息
    Map<SubClusterId, SubClusterInfo> subClusterInfoMap =
        federationFacade.getSubClusters(true);

    // 字符串格式子集群ID转换为SubClusterId类型
    SubClusterId subClusterIdKey = SubClusterId.newInstance(subClusterId);

    // 检查目标子集群是否存在且处于活跃状态
    if (!subClusterInfoMap.containsKey(subClusterIdKey)) {
      throw new YarnException("subClusterId = " + subClusterId + " is not an active subCluster.");
    }

    // 反射调用目标子集群的管理方法，处理结果
    try {
      // 获取指定子集群的RM管理协议代理
      ResourceManagerAdministrationProtocol protocol =
          rmAdminInterceptor.getAdminRMProxyForSubCluster(subClusterIdKey);
      Class<?>[] types = this.getTypes();
      Object[] params = this.getParams();
      // 反射获取目标方法
      Method method = ResourceManagerAdministrationProtocol.class.getMethod(methodName, types);
      // 反射执行方法
      Object result = method.invoke(protocol, params);
      if (result != null) {
        // 类型转换后返回单元素集合
        return Collections.singletonList(clazz.cast(result));
      }
    } catch (Exception e) {
      // 调用过程异常封装抛出
      throw new YarnException("invoke Failed, An exception occurred in subClusterId = " +
          subClusterId, e);
    }
    // 结果为空时抛出异常
    throw new YarnException("invoke Failed, An exception occurred in subClusterId = " +
        subClusterId);
  }
}