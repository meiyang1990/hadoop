// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You can obtain a copy of the License at
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

/**
 * YARN调度器迭代器选择器，用于告诉调度排序策略应该返回哪个分区的迭代器。
 * 用于支持分区调度场景下，按分区选择待调度实体集合。
 */
public class IteratorSelector {

  /** 空选择器实例，表示不选择任何分区，返回空迭代器 */
  public static final IteratorSelector EMPTY_ITERATOR_SELECTOR =
      new IteratorSelector();

  /** 目标分区名称 */
  private String partition;

  /**
   * 获取当前选择的分区名称。
   * @return 目标分区名称
   */
  public String getPartition() {
    return this.partition;
  }

  /**
   * 设置要选择的分区名称。
   * @param p 目标分区名称
   */
  public void setPartition(String p) {
    this.partition = p;
  }

}