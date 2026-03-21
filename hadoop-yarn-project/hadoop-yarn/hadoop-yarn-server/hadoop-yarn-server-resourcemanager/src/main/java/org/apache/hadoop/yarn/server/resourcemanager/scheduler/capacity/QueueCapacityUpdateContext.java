// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;

import java.util.ArrayList;
import java.util.List;

/**
 * 队列容量全量更新阶段的上下文容器，存储整个更新过程中的中间计算结果
 */
public class QueueCapacityUpdateContext {
  // 更新后的集群总资源
  private final Resource updatedClusterResource;
  // 节点标签管理器
  private final RMNodeLabelsManager labelsManager;

  // 存储更新过程中产生的警告信息列表
  private final List<QueueUpdateWarning> warnings = new ArrayList<QueueUpdateWarning>();

  /**
   * 构造队列容量更新上下文
   * @param updatedClusterResource 更新后的集群总资源
   * @param labelsManager 节点标签管理器
   */
  public QueueCapacityUpdateContext(Resource updatedClusterResource,
                                    RMNodeLabelsManager labelsManager) {
    this.updatedClusterResource = updatedClusterResource;
    this.labelsManager = labelsManager;
  }

  /**
   * 获取指定节点标签对应的更新后集群可用总资源
   *
   * @param label 节点标签
   * @return 指定标签对应的集群资源
   */
  public Resource getUpdatedClusterResource(String label) {
    return labelsManager.getResourceByLabel(label, updatedClusterResource);
  }

  /**
   * 获取空标签对应的更新后集群总资源
   * @return 空标签对应的集群资源
   */
  public Resource getUpdatedClusterResource() {
    return updatedClusterResource;
  }

  /**
   * 向上下文中添加一条更新警告
   * @param warning 更新阶段产生的警告
   */
  public void addUpdateWarning(QueueUpdateWarning warning) {
    warnings.add(warning);
  }

  /**
   * 获取当前更新阶段产生的所有警告
   * @return 更新警告列表
   */
  public List<QueueUpdateWarning> getUpdateWarnings() {
    return warnings;
  }
}