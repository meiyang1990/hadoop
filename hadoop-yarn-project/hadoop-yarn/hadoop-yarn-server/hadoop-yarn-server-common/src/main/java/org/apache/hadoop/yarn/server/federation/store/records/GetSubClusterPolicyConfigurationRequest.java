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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN联邦环境中，向FederationPolicyStore查询指定队列路由策略配置的请求类
 */
@Private
@Unstable
public abstract class GetSubClusterPolicyConfigurationRequest {

  /**
   * 创建指定队列的策略配置查询请求实例
   * @param queueName 要查询策略配置的队列名称
   * @return 新建的查询请求实例
   */
  @Private
  @Unstable
  public static GetSubClusterPolicyConfigurationRequest newInstance(
      String queueName) {
    GetSubClusterPolicyConfigurationRequest request =
        Records.newRecord(GetSubClusterPolicyConfigurationRequest.class);
    request.setQueue(queueName);
    return request;
  }

  /**
   * 获取本次请求要查询策略配置的队列名称
   *
   * @return 队列名称
   */
  @Public
  @Unstable
  public abstract String getQueue();

  /**
   * 设置本次请求要查询策略配置的队列名称
   *
   * @param queueName 队列名称
   */
  @Private
  @Unstable
  public abstract void setQueue(String queueName);
}