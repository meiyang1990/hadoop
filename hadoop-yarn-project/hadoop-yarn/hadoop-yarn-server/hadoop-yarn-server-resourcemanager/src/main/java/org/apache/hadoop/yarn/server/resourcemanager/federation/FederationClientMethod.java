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
package org.apache.hadoop.yarn.server.resourcemanager.federation;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * 文件: FederationClientMethod，YARN联邦场景下封装联邦状态存储客户端调用方法，
 * 核心职责：封装待调用方法的名称、参数类型、参数值，提供反射调用能力，
 * 用于统一封装对FederationStateStore服务端接口的反射调用，并统计调用 metrics。
 */
public class FederationClientMethod<R> {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationClientMethod.class);

  /**
   * List of parameters: static and dynamic values, matchings types.
   */
  private final Object[] params;

  /**
   * List of method parameters types, matches parameters.
   */
  private final Class<?>[] types;

  /**
   * String name of the method.
   */
  private final String methodName;

  // 联邦状态存储客户端实例
  private FederationStateStore stateStoreClient = null;

  // 时钟对象，用于统计方法调用耗时
  private Clock clock = null;

  // 方法返回值类型
  private Class<R> clazz;

  /**
   * 构造方法，封装带多参数的客户端调用方法信息。
   * @param method 方法名
   * @param pTypes 参数类型数组
   * @param pParams 参数值数组
   * @throws YarnException 参数长度不匹配时抛出异常
   */
  public FederationClientMethod(String method, Class<?>[] pTypes, Object... pParams)
      throws YarnException {
    if (pParams.length != pTypes.length) {
      throw new YarnException("Invalid parameters for method " + method);
    }

    this.params = pParams;
    this.types = Arrays.copyOf(pTypes, pTypes.length);
    this.methodName = method;
  }

  /**
   * 构造方法，封装单参数的客户端调用方法信息。
   * @param method 方法名
   * @param pTypes 参数类型
   * @param pParams 参数值
   * @throws YarnException 参数不合法时抛出异常
   */
  public FederationClientMethod(String method, Class pTypes, Object pParams)
      throws YarnException {
    this(method, new Class[]{pTypes}, new Object[]{pParams});
  }

  /**
   * 构造方法，带完整依赖注入的构造器，包含返回值类型和依赖。
   * @param method 方法名
   * @param pTypes 参数类型
   * @param pParams 参数值
   * @param rTypes 返回值类型
   * @param fedStateStore 联邦状态存储客户端实例
   * @param fedClock 时钟实例
   * @throws YarnException 参数不合法时抛出异常
   */
  public FederationClientMethod(String method, Class pTypes, Object pParams, Class<R> rTypes,
      FederationStateStore fedStateStore, Clock fedClock) throws YarnException {
    this(method, pTypes, pParams);
    this.stateStoreClient = fedStateStore;
    this.clock = fedClock;
    this.clazz = rTypes;
  }

  /**
   * 获取方法参数值数组的副本。
   * @return 方法参数值数组
   */
  public Object[] getParams() {
    return Arrays.copyOf(this.params, this.params.length);
  }

  /**
   * 获取调用方法名称。
   * @return 方法名称
   */
  public String getMethodName() {
    return methodName;
  }

  /**
   * Get the calling types for this method.
   *
   * @return An array of calling types.
   */
  public Class<?>[] getTypes() {
    return Arrays.copyOf(this.types, this.types.length);
  }

  /**
   * We will use the invoke method to call the method in FederationStateStoreService.
   *
   * @return The result returned after calling the interface.
   * @throws YarnException yarn exception.
   */
  protected R invoke() throws YarnException {
    try {
      // 记录方法调用开始时间
      long startTime = clock.getTime();
      // 通过反射获取方法对象
      Method method = FederationStateStore.class.getMethod(methodName, types);
      // 反射调用方法并强转返回结果
      R result = clazz.cast(method.invoke(stateStoreClient, params));

      // 计算调用耗时
      long stopTime = clock.getTime();
      // 上报调用成功 metrics 指标
      FederationStateStoreServiceMetrics.succeededStateStoreServiceCall(
          methodName, stopTime - startTime);
      return result;
    } catch (Exception e) {
      // 记录调用失败日志
      LOG.error("stateStoreClient call method {} error.", methodName, e);
      // 上报调用失败 metrics 指标
      FederationStateStoreServiceMetrics.failedStateStoreServiceCall(methodName);
      throw new YarnException(e);
    }
  }
}