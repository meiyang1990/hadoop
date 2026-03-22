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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 枚举类型，代表YARN调度队列的运行状态
 * 用于标识MapReduce作业提交目标队列的当前可用状态
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum QueueState {

  STOPPED("stopped"), RUNNING("running"), UNDEFINED("undefined");
  private final String stateName;
  private static Map<String, QueueState> enumMap =
      new HashMap<String, QueueState>();

  // 静态初始化：将所有队列状态按名称存入哈希表，方便快速查找
  static {
    for (QueueState state : QueueState.values()) {
      enumMap.put(state.getStateName(), state);
    }
  }

  /**
   * 构造函数，使用状态名称初始化枚举实例
   * @param stateName 状态的字符串名称
   */
  QueueState(String stateName) {
    this.stateName = stateName;
  }

  /**
   * 获取队列状态的字符串名称
   * @return 状态名称字符串
   */
  public String getStateName() {
    return stateName;
  }

  /**
   * 根据状态字符串名称查找对应的队列状态枚举
   * 如果找不到对应状态则返回UNDEFINED
   * @param state 状态名称字符串
   * @return 对应的队列状态枚举
   */
  public static QueueState getState(String state) {
    QueueState qState = enumMap.get(state);
    if (qState == null) {
      return UNDEFINED;
    }
    return qState;
  }

  @Override
  public String toString() {
    return stateName;
  }

}