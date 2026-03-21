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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 应用尝试聚合资源使用统计容器，存储各资源类型已使用的资源-秒累计值
 */
@Private
public class AggregateAppResourceUsage {
  // 按资源类型存储累计使用的资源-秒映射表
  private Map<String, Long> resourceSecondsMap = new HashMap<>();

  /**
   * 构造方法，从传入的映射表复制资源使用数据
   * @param resourceSecondsMap 预计算的资源累计使用映射表
   */
  public AggregateAppResourceUsage(Map<String, Long> resourceSecondsMap) {
    this.resourceSecondsMap.putAll(resourceSecondsMap);
  }

  /**
   * 获取内存累计使用量（单位：MB-秒）
   * @return 内存累计使用MB-秒数
   */
  public long getMemorySeconds() {
    return RMServerUtils.getOrDefault(resourceSecondsMap,
        ResourceInformation.MEMORY_MB.getName(), 0L);
  }

  /**
   * 获取CPU核心累计使用量（单位：核-秒）
   * @return CPU累计使用核-秒数
   */
  public long getVcoreSeconds() {
    return RMServerUtils
        .getOrDefault(resourceSecondsMap, ResourceInformation.VCORES.getName(),
            0L);
  }

  /**
   * 获取完整的所有资源类型累计使用映射表
   * @return 各资源类型的累计资源-秒映射表
   */
  public Map<String, Long> getResourceUsageSecondsMap() {
    return resourceSecondsMap;
  }
}