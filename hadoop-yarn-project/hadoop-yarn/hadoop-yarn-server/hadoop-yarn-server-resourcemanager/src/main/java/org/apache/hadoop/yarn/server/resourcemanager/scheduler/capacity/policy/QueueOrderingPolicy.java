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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;

import java.util.Iterator;
import java.util.List;

/**
 * 队列排序策略接口，供容量调度器的父队列对子队列进行容器分配优先级排序
 * 定义了资源分配时选择子队列的顺序规则，由{@link org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue}使用
 */
public interface QueueOrderingPolicy {
  /**
   * 设置需要排序的子队列列表，初始化策略内部结构
   * @param queues 待排序的子队列列表
   */
  void setQueues(List<CSQueue> queues);

  /**
   * 获取按排序规则排列的子队列迭代器，用于容器分配
   *
   * 注意：为了避免排序/迭代过程中子队列集合被修改，调用方必须保证已经正确获取父队列的读锁
   *
   * @param partition 节点分区标识
   * @return 按分配优先级排序的子队列迭代器
   */
  Iterator<CSQueue> getAssignmentIterator(String partition);

  /**
   * 获取该排序策略的配置名称，用于配置解析时匹配策略
   * @return 排序策略的配置名称
   */
  String getConfigName();
}