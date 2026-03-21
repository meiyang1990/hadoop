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
 * 组合比较器，将多个可调度实体比较器按顺序串联，实现多级排序规则
 * 用于YARN调度器中需要按多个维度排序的场景，前一级相等才会进入下一级比较
 */
//Some policies will use multiple comparators joined together
class CompoundComparator implements Comparator<SchedulableEntity> {

    // 按优先级排序的比较器列表，先加入的比较器优先级更高
    List<Comparator<SchedulableEntity>> comparators;

    /**
     * 构造组合比较器
     * @param comparators 按优先级排序的比较器列表
     */
    CompoundComparator(List<Comparator<SchedulableEntity>> comparators) {
      this.comparators = comparators;
    }

    @Override
    public int compare(final SchedulableEntity r1, final SchedulableEntity r2) {
      // 按顺序遍历所有比较器，依次比较
      for (Comparator<SchedulableEntity> comparator : comparators) {
        int result = comparator.compare(r1, r2);
        // 当前比较器得出不相等结果，直接返回，不再继续比较
        if (result != 0) return result;
      }
      // 所有比较器结果都相等，返回0
      return 0;
    }
}