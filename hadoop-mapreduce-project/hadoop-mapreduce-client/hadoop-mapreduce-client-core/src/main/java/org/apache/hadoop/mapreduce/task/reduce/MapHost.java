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
package org.apache.hadoop.mapreduce.task.reduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * 表示一个输出Map结果的主机节点，用于Reduce端shuffle阶段跟踪主机状态和待拉取的Map输出
 * 记录了主机上所有需要拉取的Map任务，并维护主机当前的获取状态，支持故障惩罚机制
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class MapHost {
  
  /**
   * Map主机的状态枚举，定义主机在shuffle拉取过程中的不同状态
   */
  public enum State {
    IDLE,               // 没有可用的Map输出需要拉取
    BUSY,               // 正在拉取该主机上的Map输出
    PENDING,            // 有待拉取的Map输出，等待拉取
    PENALIZED           // 因多次拉取失败被惩罚，暂时不分配拉取任务
  }
  
  private State state = State.IDLE;
  private final String hostName;
  private final String baseUrl;
  private List<TaskAttemptID> maps = new ArrayList<TaskAttemptID>();
  
  /**
   * 构造一个Map主机实例
   * @param hostName 主机名
   * @param baseUrl 该主机上Map输出的基础访问URL
   */
  public MapHost(String hostName, String baseUrl) {
    this.hostName = hostName;
    this.baseUrl = baseUrl;
  }
  
  /**
   * 获取主机当前状态
   * @return 当前状态枚举值
   */
  public State getState() {
    return state;
  }

  /**
   * 获取主机名
   * @return 主机名字符串
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * 获取Map输出访问基础URL
   * @return 基础URL字符串
   */
  public String getBaseUrl() {
    return baseUrl;
  }

  /**
   * 添加一个已知待拉取的Map任务尝试ID到该主机
   * 如果当前主机是空闲状态，自动切换为待拉取状态
   * @param mapId Map任务尝试ID
   */
  public synchronized void addKnownMap(TaskAttemptID mapId) {
    maps.add(mapId);
    if (state == State.IDLE) {
      state = State.PENDING;
    }
  }
  
  /**
   * 获取所有待拉取的Map任务并清空待拉取列表
   * 用于批量获取待拉取任务后重置列表
   * @return 当前所有待拉取的Map任务尝试ID列表
   */
  public synchronized List<TaskAttemptID> getAndClearKnownMaps() {
    List<TaskAttemptID> currentKnownMaps = maps;
    maps = new ArrayList<TaskAttemptID>();
    return currentKnownMaps;
  }
  
  /**
   * 将主机标记为忙碌状态，表示正在拉取该主机的输出
   */
  public synchronized void markBusy() {
    state = State.BUSY;
  }
  
  /**
   * 获取当前主机上待拉取的Map输出数量
   * @return 待拉取Map输出数量
   */
  public synchronized int getNumKnownMapOutputs() {
    return maps.size();
  }

  /**
   * 当惩罚结束或拷贝完成后，将主机标记为可用状态
   * 根据是否还有待拉取输出自动设置为空闲或待拉取状态
   * @return 主机更新后的新状态
   */
  public synchronized State markAvailable() {
    if (maps.isEmpty()) {
      state = State.IDLE;
    } else {
      state = State.PENDING;
    }
    return state;
  }
  
  @Override
  public String toString() {
    return hostName;
  }
  
  /**
   * 将主机标记为惩罚状态，因shuffle拉取失败
   * 惩罚期间不会分配新的拉取任务，避免浪费资源
   */
  public synchronized void penalize() {
    state = State.PENALIZED;
  }
}