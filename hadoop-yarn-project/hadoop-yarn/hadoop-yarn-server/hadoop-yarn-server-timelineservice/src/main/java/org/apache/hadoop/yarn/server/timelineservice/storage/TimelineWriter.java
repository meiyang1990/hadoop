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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;

/**
 * 时间线服务写入存储层接口，定义了时间线数据持久化的统一操作规范。
 * 负责处理应用时间线信息的写入、聚合、刷新和健康检查操作，不同存储后端实现该接口提供存储能力。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface TimelineWriter extends Service {

  /**
   * 将一组时间线实体数据写入底层存储，单条写入错误会返回在响应中不影响整体操作。
   *
   * @param context 时间线收集器上下文，包含流、应用等上下文信息
   * @param data 待写入的时间线实体集合
   * @param callerUgi 调用者用户信息，用于权限检查
   * @return 写入响应，包含写入结果和错误信息
   * @throws IOException 写入底层存储时遇到IO异常
   */
  TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineEntities data, UserGroupInformation callerUgi) throws IOException;

  /**
   * 将单个时间线域信息写入底层存储，写入错误会返回在响应中。
   *
   * @param context 时间线收集器上下文，包含流、应用等上下文信息
   * @param domain 待写入的时间线域对象
   * @return 写入响应，包含写入结果和错误信息
   * @throws IOException 写入底层存储时遇到IO异常
   */
  TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineDomain domain) throws IOException;

  /**
   * 按指定聚合维度对时间线实体信息进行聚合写入，当前未实际使用，所有实现均返回null。
   *
   * @param data 待聚合的时间线实体对象
   * @param track 聚合维度轨道，指定按哪个维度聚合（如用户、流、队列等）
   * @return 聚合写入响应，所有实现均返回null
   * @throws IOException 聚合写入底层存储时遇到IO异常
   */
  TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) throws IOException;

  /**
   * 将缓冲区中的所有数据刷新写入到底层存储，可能会耗时较长需谨慎调用。
   *
   * @throws IOException 刷新数据到后端存储时遇到IO异常
   */
  void flush() throws IOException;

  /**
   * 检查写入器与底层存储的连接健康状态。
   *
   * @return 时间线健康状态，包含连接是否正常的信息
   */
  TimelineHealth getHealthStatus();


}