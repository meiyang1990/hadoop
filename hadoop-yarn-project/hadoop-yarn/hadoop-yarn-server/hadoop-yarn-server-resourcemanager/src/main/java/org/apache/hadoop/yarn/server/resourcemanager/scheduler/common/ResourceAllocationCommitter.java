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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.Resource;

/**
 * YARN调度器资源分配提交接口，支持多线程并行调度结合全局提交校验的调度模式
 * 希望实现多线程调度+全局资源校验功能的调度器需要实现该接口
 */
public interface ResourceAllocationCommitter {

  /**
   * 尝试提交资源分配提案，校验并确认资源分配是否生效
   * @param cluster 集群当前总资源信息
   * @param proposal 待提交的资源分配提案
   * @param updatePending 提交成功后是否需要递减待分配请求计数
   * @return 提交是否成功，true表示分配生效，false表示分配失败需要回滚
   */
  boolean tryCommit(Resource cluster, ResourceCommitRequest proposal,
      boolean updatePending);
}