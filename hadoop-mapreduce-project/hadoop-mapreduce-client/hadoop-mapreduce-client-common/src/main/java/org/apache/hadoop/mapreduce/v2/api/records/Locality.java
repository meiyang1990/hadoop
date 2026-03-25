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

package org.apache.hadoop.mapreduce.v2.api.records;

/**
 * 任务数据局部性等级枚举，定义MapReduce任务调度中不同层级的数据局部性类型
 * 用于调度器根据任务输入数据所在位置，选择最优的任务执行节点，减少网络传输开销
 */
public enum Locality {
  /** 数据和任务运行在同一个节点，最高局部性，最少网络开销 */
  NODE_LOCAL,
  /** 数据和任务运行在同一个机架不同节点，中等局部性，同一机架内网络开销较低 */
  RACK_LOCAL,
  /** 数据和任务运行在不同机架，最低局部性，需要跨机架网络传输，开销最大 */
  OFF_SWITCH
}