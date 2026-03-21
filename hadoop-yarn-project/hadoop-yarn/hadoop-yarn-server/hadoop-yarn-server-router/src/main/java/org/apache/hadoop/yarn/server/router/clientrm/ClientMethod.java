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
package org.apache.hadoop.yarn.server.router.clientrm;

import java.io.IOException;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 封装客户端调用ResourceManager服务的方法信息，包含方法名、参数类型和参数值。
 * 用于YARN Router转发客户端请求到对应RM时存储方法调用信息。
 */
public class ClientMethod {

  private static final Logger LOG = LoggerFactory.getLogger(ClientMethod.class);
  /**
   * 方法参数值数组，按顺序存储所有传入的参数。
   */
  private final Object[] params;
  /**
   * 方法参数类型数组，与参数值一一对应。
   */
  private final Class<?>[] types;
  /**
   * 调用的方法名称字符串。
   */
  private final String methodName;

  /**
   * 构造方法，初始化方法调用信息并参数校验。
   * @param method 方法名称
   * @param pTypes 参数类型数组
   * @param pParams 参数值数组
   * @throws IOException 参数数量不匹配时抛出异常
   */
  public ClientMethod(String method, Class<?>[] pTypes, Object... pParams)
      throws IOException {
    if (pParams.length != pTypes.length) {
      throw new IOException("Invalid parameters for method " + method);
    }

    this.params = pParams;
    this.types = Arrays.copyOf(pTypes, pTypes.length);
    this.methodName = method;
  }

  /**
   * 获取方法参数值数组的拷贝。
   * @return 参数值数组拷贝
   */
  public Object[] getParams() {
    return Arrays.copyOf(this.params, this.params.length);
  }

  /**
   * 获取调用的方法名称。
   * @return 方法名称
   */
  public String getMethodName() {
    return methodName;
  }

  /**
   * 获取方法参数类型数组的拷贝。
   *
   * @return 参数类型数组拷贝
   */
  public Class<?>[] getTypes() {
    return Arrays.copyOf(this.types, this.types.length);
  }
}