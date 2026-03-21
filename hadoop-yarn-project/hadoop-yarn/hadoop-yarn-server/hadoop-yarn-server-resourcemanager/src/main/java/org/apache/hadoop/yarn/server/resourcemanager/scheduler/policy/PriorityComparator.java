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

import org.apache.hadoop.yarn.api.records.Priority;

/**
 * 按优先级对可调度实体进行排序的比较器，用于YARN调度器中调度队列排序等待分配的应用/容器。
 */
public class PriorityComparator implements Comparator<SchedulableEntity> {

  @Override
  public int compare(SchedulableEntity se1, SchedulableEntity se2) {
    Priority p1 = se1.getPriority();
    Priority p2 = se2.getPriority();
    // 两个对象优先级都为空时，视为相等
    if (p1 == null && p2 == null) {
      return 0;
    } else if (p1 == null) {
      // 第一个优先级为空，认为优先级更低排在后面
      return -1;
    } else if (p2 == null) {
      // 第二个优先级为空，认为第一个优先级更高排在前面
      return 1;
    }
    // 委托Priority自身的比较方法完成优先级比较
    return p1.compareTo(p2);
  }
}