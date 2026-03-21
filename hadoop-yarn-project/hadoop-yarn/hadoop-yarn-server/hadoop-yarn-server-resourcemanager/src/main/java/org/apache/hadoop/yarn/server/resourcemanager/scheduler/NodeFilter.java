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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * YARN资源调度器节点过滤接口，根据指定条件筛选集群节点，配合{@link ClusterNodeTracker}使用完成节点过滤
 */
@InterfaceAudience.Private
public interface NodeFilter {

  /**
   * 判断节点是否符合过滤条件，判断是否将节点加入过滤结果列表
   *
   * @param node 调度器节点对象
   * @return true 保留该节点; false 过滤掉该节点
   */
  boolean accept(SchedulerNode node);
}