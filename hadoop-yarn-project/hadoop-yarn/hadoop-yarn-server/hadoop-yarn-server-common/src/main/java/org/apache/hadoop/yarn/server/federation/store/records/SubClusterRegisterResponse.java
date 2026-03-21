// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * 子集群注册响应类，封装联邦集群成员状态存储对ResourceManager注册请求的返回结果。
 * 操作成功时响应为空，失败会通过异常返回具体失败原因。
 */
@Private
@Unstable
public abstract class SubClusterRegisterResponse {

  /**
   * 创建一个新的子集群注册响应实例。
   * @return 新建的空响应实例
   */
  @Private
  @Unstable
  public static SubClusterRegisterResponse newInstance() {
    SubClusterRegisterResponse response =
        Records.newRecord(SubClusterRegisterResponse.class);
    return response;
  }

}