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
package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * 删除子集群路由策略配置请求类，封装待删除策略关联的队列列表，用于YARN联邦状态存储层处理策略删除操作。
 */
@Private
@Unstable
public abstract class DeleteSubClusterPoliciesConfigurationsRequest {

  /**
   * 创建删除子集群策略配置请求实例，传入待删除策略关联的队列列表。
   *
   * @param queues 待删除策略关联的队列名称列表
   * @return 初始化完成的删除请求对象
   */
  @Private
  @Unstable
  public static DeleteSubClusterPoliciesConfigurationsRequest newInstance(
      List<String> queues) {
    DeleteSubClusterPoliciesConfigurationsRequest request =
        Records.newRecord(DeleteSubClusterPoliciesConfigurationsRequest.class);
    request.setQueues(queues);
    return request;
  }

  /**
   * 获取待删除策略关联的队列名称列表。
   *
   * @return 队列名称列表
   */
  @Public
  @Unstable
  public abstract List<String> getQueues();

  /**
   * 设置待删除策略关联的队列名称列表。
   *
   * @param queues 队列名称列表
   */
  @Private
  @Unstable
  public abstract void setQueues(List<String> queues);
}