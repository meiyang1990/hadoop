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
 * YARN联邦路由策略配置删除请求类，封装删除路由策略配置的请求参数。
 * 用于向联邦状态存储发起删除所有策略配置的请求。
 */
public abstract class DeletePoliciesConfigurationsRequest {

  /**
   * 创建一个新的删除策略配置请求实例。
   * @return 新建的DeletePoliciesConfigurationsRequest实例
   */
  @Private
  @Unstable
  public static DeletePoliciesConfigurationsRequest newInstance() {
    return Records.newRecord(DeletePoliciesConfigurationsRequest.class);
  }
}