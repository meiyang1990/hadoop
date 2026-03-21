// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.UNDEFINED;

/**
 * 容量调度器队列资源分配配置容器，负责根据调度器配置和层级关系，计算队列最小/最大资源分配限制。
 **/
public class QueueAllocationSettings {
  private final Resource minimumAllocation;
  private Resource maximumAllocation;

  /**
   * 构造队列分配配置对象，初始化最小资源分配。
   * @param minimumAllocation 队列最小资源分配
   */
  public QueueAllocationSettings(Resource minimumAllocation) {
    this.minimumAllocation = minimumAllocation;
  }

  /**
   * 根据配置和父队列设置，计算当前队列的最大资源分配限制。
   * @param configuration 容量调度器配置
   * @param queuePath 队列路径
   * @param parent 父队列，根队列为null
   */
  void setupMaximumAllocation(CapacitySchedulerConfiguration configuration, QueuePath queuePath,
      CSQueue parent) {
    // 从配置中获取集群层面最大资源分配
    Resource clusterMax = ResourceUtils
        .fetchMaximumAllocationFromConfig(configuration);
    // 从配置中获取当前队列配置的最大资源分配
    Resource queueMax = configuration.getQueueMaximumAllocation(queuePath);

    // 初始最大资源分配继承自父队列，根队列则使用集群最大配置
    maximumAllocation = Resources.clone(
        parent == null ? clusterMax : parent.getMaximumAllocation());

    // 错误信息模板：队列最大分配不能超过集群配置
    String errMsg =
        "Queue maximum allocation cannot be larger than the cluster setting"
            + " for queue " + queuePath
            + " max allocation per queue: %s"
            + " cluster setting: " + clusterMax;

    // 处理向后兼容：新格式未配置时，读取旧格式配置
    if (queueMax == Resources.none()) {
      // 读取旧版内存和vcore配置
      long queueMemory = configuration.getQueueMaximumAllocationMb(queuePath);
      int queueVcores = configuration.getQueueMaximumAllocationVcores(queuePath);
      // 配置存在则更新最大分配中的内存值
      if (queueMemory != UNDEFINED) {
        maximumAllocation.setMemorySize(queueMemory);
      }

      // 配置存在则更新最大分配中的vcore值
      if (queueVcores != UNDEFINED) {
        maximumAllocation.setVirtualCores(queueVcores);
      }

      // 校验配置不超过集群最大限制，超出则抛出异常
      if ((queueMemory != UNDEFINED && queueMemory > clusterMax.getMemorySize()
          || (queueVcores != UNDEFINED
          && queueVcores > clusterMax.getVirtualCores()))) {
        throw new IllegalArgumentException(
            String.format(errMsg, maximumAllocation));
      }
    } else {
      // 新格式配置：逐个资源校验队列最大分配不超过集群配置
      for (ResourceInformation ri : queueMax.getResources()) {
        // 当前资源队列配置大于集群配置，抛出异常
        if (ri.compareTo(clusterMax.getResourceInformation(ri.getName())) > 0) {
          throw new IllegalArgumentException(String.format(errMsg, queueMax));
        }

        // 更新最大分配中的对应资源值
        maximumAllocation.setResourceInformation(ri.getName(), ri);
      }
    }
  }

  /**
   * 获取队列最小资源分配。
   * @return 最小资源分配
   */
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  /**
   * 获取队列最大资源分配。
   * @return 最大资源分配
   */
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
}