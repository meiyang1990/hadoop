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
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 待分配资源请求，保存特定约束条件（如特定主机/机架/节点属性）下的资源请求最小信息，用于YARN调度器处理等待分配的资源请求
 */
public class PendingAsk {
  // 单次分配所需的资源量
  private final Resource perAllocationResource;
  // 需要分配的次数
  private final int count;
  // 空资源请求常量，代表无资源请求
  public final static PendingAsk ZERO = new PendingAsk(Resources.none(), 0);

  /**
   * 通过资源规格构造待分配资源请求
   * @param sizing 资源规格，包含单次资源量和分配次数
   */
  public PendingAsk(ResourceSizing sizing) {
    this.perAllocationResource = sizing.getResources();
    this.count = sizing.getNumAllocations();
  }

  /**
   * 通过指定资源量和分配次数构造待分配资源请求
   * @param res 单次分配所需资源量
   * @param num 需要分配的次数
   */
  public PendingAsk(Resource res, int num) {
    this.perAllocationResource = res;
    this.count = num;
  }

  /**
   * 获取单次分配所需资源量
   * @return 单次分配资源量
   */
  public Resource getPerAllocationResource() {
    return perAllocationResource;
  }

  /**
   * 获取需要分配的次数
   * @return 分配次数
   */
  public int getCount() {
    return count;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<per-allocation-resource=")
        .append(getPerAllocationResource())
        .append(",repeat=")
        .append(getCount())
        .append(">");
    return sb.toString();
  }
}