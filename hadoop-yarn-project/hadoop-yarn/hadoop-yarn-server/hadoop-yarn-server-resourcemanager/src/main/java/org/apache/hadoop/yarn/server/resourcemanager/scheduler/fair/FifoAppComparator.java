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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * FIFO调度器应用比较器，按照优先级 -> 提交时间 -> 应用ID的顺序对公平调度器中的应用进行排序
 * 实现Hadoop默认调度器的排序规则，用于FIFO调度策略下的应用排序。
 */
@Private
@Unstable
public class FifoAppComparator implements Comparator<FSAppAttempt>, Serializable {
  private static final long serialVersionUID = 34288350833489547918L;

  /**
   * 比较两个公平调度应用尝试的排序优先级
   * @param a1 第一个应用尝试
   * @param a2 第二个应用尝试
   * @return 比较结果，负数表示a1优先，正数表示a2优先，0表示相等
   */
  public int compare(FSAppAttempt a1, FSAppAttempt a2) {
    // 首先比较应用优先级
    int res = a1.getPriority().compareTo(a2.getPriority());
    if (res == 0) {
      // 优先级相同则比较提交时间，更早提交的排在前面
      if (a1.getStartTime() < a2.getStartTime()) {
        res = -1;
      } else {
        res = (a1.getStartTime() == a2.getStartTime() ? 0 : 1);
      }
    }
    if (res == 0) {
      // 如果优先级和提交时间都相同，通过应用ID breaking tie，保证排序结果确定性
      res = a1.getApplicationId().compareTo(a2.getApplicationId());
    }
    return res;
  }
}