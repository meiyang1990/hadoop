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

package org.apache.hadoop.yarn.server.timelineservice.storage;

/**
 * 时间线数据聚合维度枚举，定义了实体信息需要按哪些维度进行聚合统计
 * <p>
 * 在YARN时间线服务中，用于指定不同层级的聚合维度，支持按应用、流、用户、队列四个维度分别聚合指标
 * </p>
 */
public enum TimelineAggregationTrack {
  /** 按应用维度聚合 */
  APP,
  /** 按工作流维度聚合 */
  FLOW,
  /** 按用户维度聚合 */
  USER,
  /** 按队列维度聚合 */
  QUEUE
}