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

/**
 * YARN调度器节点位置类型枚举，用于调度时根据数据位置进行资源分配优化
 * 按照数据locality层级划分，优先选择距离数据近的节点分配资源
 */
public enum NodeType {
  /** 节点本地：数据就在当前节点，延迟最低 */
  NODE_LOCAL(0),
  /** 机架本地：数据在同一个机架的其他节点，延迟较低 */
  RACK_LOCAL(1),
  /** 跨机架：数据在不同机架，延迟最高 */
  OFF_SWITCH(2);

  private final int index;

  NodeType(int index) {
    this.index = index;
  }

  /**
   * @return 获取节点类型的索引值，索引越小优先级越高
   */
  public int getIndex() {
    return index;
  }
}