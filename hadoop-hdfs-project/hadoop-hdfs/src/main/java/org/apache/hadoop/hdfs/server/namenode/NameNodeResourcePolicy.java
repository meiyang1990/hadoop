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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS NameNode 资源可用性检查策略类，基于一组可检查的资源判断当前是否有足够资源供NameNode继续运行
 */
@InterfaceAudience.Private
final class NameNodeResourcePolicy {

  /**
   * 判断NameNode是否有足够资源继续写入编辑日志
   * 
   * @param resources 待检查的资源集合
   * @param minimumRedundantResources 继续运行所需的最少冗余可用资源数量
   * @return 资源足够返回true，否则返回false
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(NameNodeResourcePolicy.class.getName());

  static boolean areResourcesAvailable(
      Collection<? extends CheckableNameNodeResource> resources,
      int minimumRedundantResources) {

    // TODO: 兼容性处理：启动阶段如果还没有任何编辑日志目录，直接返回可用，避免误进入安全模式
    if (resources.isEmpty()) {
      return true;
    }
    
    int requiredResourceCount = 0;
    int redundantResourceCount = 0;
    int disabledRedundantResourceCount = 0;
    // 遍历所有待检查资源，分类统计可用状态
    for (CheckableNameNodeResource resource : resources) {
      if (!resource.isRequired()) {
        // 统计冗余资源总数
        redundantResourceCount++;
        if (!resource.isResourceAvailable()) {
          // 统计不可用的冗余资源数量
          disabledRedundantResourceCount++;
        }
      } else {
        // 统计必需资源总数
        requiredResourceCount++;
        if (!resource.isResourceAvailable()) {
          // 存在必需资源不可用，直接返回资源不足
          return false;
        }
      }
    }
    
    if (redundantResourceCount == 0) {
      // 没有配置冗余资源，只要存在可用必需资源即返回可用
      return requiredResourceCount > 0;
    } else {
      // 计算可用冗余资源是否满足最小要求
      final boolean areResourceAvailable =
          redundantResourceCount - disabledRedundantResourceCount >= minimumRedundantResources;
      // 资源不足时记录日志供运维排查
      if (!areResourceAvailable) {
        LOG.info("Resources not available. Details: redundantResourceCount={},"
                + " disabledRedundantResourceCount={}, minimumRedundantResources={}.",
            redundantResourceCount, disabledRedundantResourceCount, minimumRedundantResources);
      }
      return areResourceAvailable;
    }
  }
}