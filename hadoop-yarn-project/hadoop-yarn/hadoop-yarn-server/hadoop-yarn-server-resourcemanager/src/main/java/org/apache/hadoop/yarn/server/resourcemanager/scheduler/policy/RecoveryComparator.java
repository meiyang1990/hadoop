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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Comparator;

/**
 * 对可调度实体基于恢复状态进行排序的比较器
 * 保证正在恢复中的实体排在未恢复实体后面，让已恢复实体先得到调度
 */
public class RecoveryComparator implements Comparator<SchedulableEntity> {
  @Override
  public int compare(SchedulableEntity se1, SchedulableEntity se2) {
    // 将第一个实体的恢复状态转换为数值，正在恢复为1，否则为0
    int val1 = se1.isRecovering() ? 1 : 0;
    // 将第二个实体的恢复状态转换为数值，正在恢复为1，否则为0
    int val2 = se2.isRecovering() ? 1 : 0;
    // 降序排序：正在恢复的实体排在未恢复实体之后
    return val2 - val1;
  }
}