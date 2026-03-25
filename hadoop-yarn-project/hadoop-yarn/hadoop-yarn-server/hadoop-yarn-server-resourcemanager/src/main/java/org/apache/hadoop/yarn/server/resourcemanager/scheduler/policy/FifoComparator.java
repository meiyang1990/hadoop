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

import java.util.*;

/**
 * 文件说明：FIFO调度策略比较器，实现YARN调度可调度实体的先进先出排序
 * 
 * 比较器功能：按照先入先出顺序对可调度实体进行排序，先按输入顺序比较，输入顺序相同时再按启动时间排序
 */
public class FifoComparator 
    implements Comparator<SchedulableEntity> {
      
    /**
     * 比较两个可调度实体的优先级，用于FIFO排序
     * @param r1 第一个可调度实体
     * @param r2 第二个可调度实体
     * @return 比较结果：负数表示r1排在r2前，正数表示r1排在r2后，0表示相等
     */
    @Override
  public int compare(SchedulableEntity r1, SchedulableEntity r2) {
    // 首先使用输入顺序进行比较
    int res = r1.compareInputOrderTo(r2);

    // 输入顺序相同时，使用启动时间排序，先启动的排在前面
    if (res == 0) {
      res = (int) Math.signum(r1.getStartTime() - r2.getStartTime());
    }

    return res;
  }
}