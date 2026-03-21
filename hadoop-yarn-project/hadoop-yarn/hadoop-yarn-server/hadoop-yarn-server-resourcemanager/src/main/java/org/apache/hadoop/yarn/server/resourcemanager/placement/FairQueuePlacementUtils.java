// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this name except in compliance
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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：公平调度器队列放置规则工具类，提供队列名称清洗、格式化和校验工具能力
 * Utility methods used by Fair scheduler placement rules.
 * {@link
 * org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler}
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class FairQueuePlacementUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(FairQueuePlacementUtils.class);

  // 队列名称清理与层级检查常量定义
  protected static final String DOT = ".";
  protected static final String DOT_REPLACEMENT = "_dot_";
  protected static final String ROOT_QUEUE = "root";

  private FairQueuePlacementUtils() {
  }

  /**
   * 清洗用户名/组名，将点替换为下划线，去除首尾空格，用于生成合法队列名称
   *
   * @param name 待清洗的原始名称
   * @return 清洗后的合法队列名称
   */
  protected static String cleanName(String name) {
    name = FairSchedulerUtilities.trimQueueName(name);
    if (name.contains(DOT)) {
      String converted = name.replaceAll("\\.", DOT_REPLACEMENT);
      LOG.warn("Name {} is converted to {} when it is used as a queue name.",
          name, converted);
      return converted;
    } else {
      return name;
    }
  }

  /**
   * 确保队列名称带有root前缀，保证全路径合法性，自动补全root前缀
   *
   * @param queueName 待处理的队列名称
   * @return 带有root前缀的完整队列路径
   */
  protected static String assureRoot(String queueName) {
    if (queueName != null && !queueName.isEmpty()) {
      if (!queueName.startsWith(ROOT_QUEUE + DOT) &&
          !queueName.equals(ROOT_QUEUE)) {
        queueName = ROOT_QUEUE + DOT + queueName;
      }
    } else {
      LOG.warn("AssureRoot: queueName is empty or null.");
    }
    return queueName;
  }

  /**
   * 校验队列名称是否合法，不允许以点开头或结尾，不允许首尾存在空格
   *
   * @param queueName 待校验的队列名称
   * @return <code>false</code> 如果队列名称以点开头/结尾，或首尾存在空格，否则返回<code>true</code>
   */
  protected static boolean isValidQueueName(String queueName) {
    if (queueName != null) {
      if (queueName.equals(FairSchedulerUtilities.trimQueueName(queueName)) &&
          !queueName.startsWith(DOT) &&
          !queueName.endsWith(DOT)) {
        return true;
      }
    }
    return false;
  }
}