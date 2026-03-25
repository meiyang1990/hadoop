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
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;


/**
 * 文件说明：YARN资源调度器可调度实体排序策略接口
 * 
 * OrderingPolicy 被调度器用于对可调度实体进行排序，用于容器分配和抢占流程
 * @param <S> 参与比较的{@link SchedulableEntity}子类型
 */
public interface OrderingPolicy<S extends SchedulableEntity> {
  /*
   * Note: OrderingPolicy depends upon external
   * synchronization of all use of the SchedulableEntity Collection and
   * Iterators for correctness and to avoid concurrent modification issues
   */

  /**
   * 获取该排序策略管理的所有可调度实体集合，不保证顺序
   * @return 可调度实体集合
   */
  public Collection<S> getSchedulableEntities();

  /**
   * 获取用于容器分配的可调度实体迭代器，已按排序策略排序
   * @param sel 迭代器过滤器，用于过滤符合条件的实体
   * @return 排序后的可调度实体迭代器
   */
  Iterator<S> getAssignmentIterator(IteratorSelector sel);

  /**
   * 获取用于抢占的可调度实体迭代器，已按排序策略排序
   * @return 排序后的可调度实体迭代器
   */
  public Iterator<S> getPreemptionIterator();

  /**
   * 添加一个可调度实体到排序策略中进行管理，用于分配和抢占排序
   * @param s 要添加的可调度实体
   */
  public void addSchedulableEntity(S s);

  /**
   * 从排序策略中移除一个可调度实体，不再参与分配和抢占排序
   * @param s 要移除的可调度实体
   * @return 该实体本次移除前是否存在于集合中
   */
  public boolean removeSchedulableEntity(S s);

  /**
   * 批量添加一批可调度实体到排序策略中进行管理
   * @param sc 要添加的可调度实体集合
   */
  public void addAllSchedulableEntities(Collection<S> sc);

  /**
   * 获取当前排序策略管理的可调度实体总数
   * @return 可调度实体数量
   */
  public int getNumSchedulableEntities();

  /**
   * 从调度器配置中加载策略配置
   * @param conf 调度器配置键值对
   */
  public void configure(Map<String, String> conf);

  /**
   * 通知排序策略：可调度实体分配到了新容器，部分排序策略需要根据此信息重新排序
   * @param schedulableEntity 分配到容器的可调度实体
   * @param r 分配得到的RMContainer
   */
  public void containerAllocated(S schedulableEntity, RMContainer r);

  /**
   * 通知排序策略：可调度实体释放了一个容器，部分排序策略需要根据此信息重新排序
   * @param schedulableEntity 释放容器的可调度实体
   * @param r 释放的RMContainer
   */
  public void containerReleased(S schedulableEntity, RMContainer r);

  /**
   * 通知排序策略：可调度实体的资源需求已更新，允许排序策略按需重新排序
   * @param schedulableEntity 需求更新后的可调度实体
   */
  void demandUpdated(S schedulableEntity);

  /**
   * 获取排序策略的配置与状态信息
   * @return 配置状态信息字符串
   */
  public String getInfo();

  /**
   * 获取排序策略配置名称，用于在配置中指定该排序策略
   * @return 排序策略配置名称
   */
  String getConfigName();

}