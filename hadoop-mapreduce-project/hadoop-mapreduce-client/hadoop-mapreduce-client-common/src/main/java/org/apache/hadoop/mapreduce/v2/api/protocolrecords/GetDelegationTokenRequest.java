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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * 获取MapReduce服务委派令牌的请求接口，定义了请求参数的存取方法。
 * 委派令牌用于Hadoop集群中的身份认证，允许后续操作代表原用户执行。
 */
@Public
@Evolving
public interface GetDelegationTokenRequest {
  /**
   * 获取令牌更新者名称，该用户/服务拥有更新此委派令牌的权限。
   * @return 允许更新令牌的主体名称
   */
  String getRenewer();
  
  /**
   * 设置令牌更新者名称，指定可以更新此委派令牌的用户/服务。
   * @param renewer 允许更新令牌的主体名称
   */
  void setRenewer(String renewer);
}