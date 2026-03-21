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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

/**
 * YARN应用队列放置上下文，封装应用放置到目标队列后的位置信息，包含目标队列名称和父队列名称。
 * 放置规则成功匹配应用后会返回该对象，用于描述应用最终应该放置到队列树中的哪个位置。
 */
public class ApplicationPlacementContext {

  private String queue;

  private String parentQueue;

  /**
   * 构造只有目标队列名称、没有父队列的放置上下文（对应根队列下的直接子队列）。
   * @param queue 目标队列名称
   */
  public ApplicationPlacementContext(String queue) {
    this(queue,null);
  }

  /**
   * 构造包含目标队列和父队列的放置上下文。
   * @param queue 目标队列名称
   * @param parentQueue 父队列名称
   */
  public ApplicationPlacementContext(String queue, String parentQueue) {
    this.queue = queue;
    this.parentQueue = parentQueue;
  }

  /**
   * 获取目标队列名称。
   * @return 目标队列名称
   */
  public String getQueue() {
    return queue;
  }

  public void setQueue(String q) {
    queue = q;
  }

  /**
   * 获取父队列名称。
   * @return 父队列名称，若没有父队列则返回null
   */
  public String getParentQueue() {
    return parentQueue;
  }

  /**
   * 判断当前队列是否存在父队列。
   * @return true如果存在父队列，否则false
   */
  public boolean hasParentQueue() {
    return parentQueue != null;
  }

  /**
   * 获取队列完整路径，父队列和子队列用点分隔。
   * @return 完整队列路径
   */
  public String getFullQueuePath() {
    if (parentQueue != null) {
      return parentQueue + "." + queue;
    } else {
      return queue;
    }
  }

}