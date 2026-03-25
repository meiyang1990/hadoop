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

import org.slf4j.Logger;

/**
 * YARN资源调度器应用工具类，提供调度相关的公共工具方法。
 */
public class SchedulerAppUtils {

  /**
   * 检查节点或所在机架是否被当前应用拉入放置黑名单，即禁止在该节点分配容器。
   * @param application 调度器中的应用尝试实例
   * @param node 待检查的节点
   * @param log 日志对象，用于输出调试信息
   * @return 如果节点或机架被拉黑返回true，否则返回false
   */
  public static boolean isPlaceBlacklisted(
      SchedulerApplicationAttempt application, SchedulerNode node,
      Logger log) {
    if (application.isPlaceBlacklisted(node.getNodeName())) {
      log.debug("Skipping 'host' {} for {} since it has been blacklisted",
          node.getNodeName(), application.getApplicationId());
      return true;
    }

    if (application.isPlaceBlacklisted(node.getRackName())) {
      log.debug("Skipping 'rack' {} for {} since it has been blacklisted",
          node.getRackName(), application.getApplicationId());
      return true;
    }

    return false;
  }

}