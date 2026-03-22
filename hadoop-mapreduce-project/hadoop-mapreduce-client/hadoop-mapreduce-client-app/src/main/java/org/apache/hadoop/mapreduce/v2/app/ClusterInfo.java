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

package org.apache.hadoop.mapreduce.v2.app;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

/**
 * 存储YARN集群资源能力信息，为MapReduce应用Master资源分配提供集群能力参考
 * 核心保存集群单容器可分配的最大资源配额，用于任务资源请求合理性校验
 */
@InterfaceAudience.LimitedPrivate("MapReduce")
public class ClusterInfo {
  private Resource maxContainerCapability;

  /**
   * 默认构造函数，初始化空的最大容器资源配置
   */
  public ClusterInfo() {
    this.maxContainerCapability = Records.newRecord(Resource.class);
  }

  /**
   * 带参数构造函数，使用指定的最大容器资源创建集群信息对象
   * @param maxCapability 集群允许单个容器使用的最大资源配额
   */
  public ClusterInfo(Resource maxCapability) {
    this.maxContainerCapability = maxCapability;
  }

  /**
   * 获取集群允许单个容器使用的最大资源配额
   * @return 单容器最大资源能力对象
   */
  public Resource getMaxContainerCapability() {
    return maxContainerCapability;
  }

  /**
   * 设置集群允许单个容器使用的最大资源配额
   * @param maxContainerCapability 待设置的单容器最大资源能力
   */
  public void setMaxContainerCapability(Resource maxContainerCapability) {
    this.maxContainerCapability = maxContainerCapability;
  }
}