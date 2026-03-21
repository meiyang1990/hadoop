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

package org.apache.hadoop.yarn.server.federation.utils;

import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * YARN联邦环境中方法调用的抽象包装基类，用于统一管理跨子集群方法调用的参数信息，
 * 支持并发调用多个子集群方法并收集结果。
 */
public abstract class FederationMethodWrapper {

  /**
   * List of parameters: static and dynamic values, matching types.
   */
  private Object[] params;

  /**
   * List of method parameters types, matches parameters.
   */
  private Class<?>[] types;

  /**
   * String name of the method.
   */
  private String methodName;

  /**
   * 构造方法包装器，校验参数和参数类型长度匹配。
   * @param pTypes 参数类型数组
   * @param pParams 参数值数组
   * @throws IOException 参数长度不匹配时抛出异常
   */
  public FederationMethodWrapper(Class<?>[] pTypes, Object... pParams)
      throws IOException {
    if (pParams.length != pTypes.length) {
      throw new IOException("Invalid parameters for method.");
    }
    this.params = pParams;
    this.types = Arrays.copyOf(pTypes, pTypes.length);
  }

  public Object[] getParams() {
    return Arrays.copyOf(this.params, this.params.length);
  }

  public String getMethodName() {
    return methodName;
  }

  public void setMethodName(String methodName) {
    this.methodName = methodName;
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
   * 抽象方法，并发调用多个子集群上的目标方法并收集返回结果。
   * @param clazz 返回结果类型Class对象
   * @param <R> 返回结果泛型
   * @return 所有子集群调用返回结果集合
   * @throws YarnException 调用过程中发生Yarn异常抛出
   */
  protected abstract <R> Collection<R> invokeConcurrent(Class<R> clazz) throws YarnException;
}