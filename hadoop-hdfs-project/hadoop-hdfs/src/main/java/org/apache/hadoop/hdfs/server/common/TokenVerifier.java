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
package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

/**
 * 文件说明：HDFS服务端通用组件，提供WebHDFS访问中的代理令牌验证接口
 * 
 * 接口作用：定义代理令牌验证契约，供NameNode、Router等服务端组件实现，
 * 用于验证通过WebHDFS接口传入的代理令牌合法性，保障访问安全。
 * 具体验证逻辑由实现类提供，JspHelper会调用该接口完成令牌验证。
 * 
 * @param <T> 具体的代理令牌标识符类型，需继承自AbstractDelegationTokenIdentifier
 */
public interface TokenVerifier<T extends AbstractDelegationTokenIdentifier> {

  /**
   * 验证通过WebHDFS传入的代理令牌合法性
   * 由NameNode、Router等核心组件实现该方法，供JspHelper调用完成验证
   * 
   * @param t 待验证的代理令牌标识符
   * @param password 代理令牌对应的密码字节数组
   * @throws IOException 验证失败或IO异常时抛出
   */
  void verifyToken(T t, byte[] password) throws IOException;

}