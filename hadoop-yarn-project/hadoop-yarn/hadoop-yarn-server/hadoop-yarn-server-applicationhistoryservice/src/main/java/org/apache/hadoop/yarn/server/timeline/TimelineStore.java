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

package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;

/**
 * YARN Timeline服务数据存储抽象接口，组合了数据读取、写入和生命周期管理能力。
 * 是Timeline应用指标历史服务的核心存储层抽象，定义了时序数据存储的统一契约，
 * 允许不同存储后端（内存、LevelDB、关系数据库等）实现该接口。
 */
@Private
@Unstable
public interface TimelineStore extends
    Service, TimelineReader, TimelineWriter {

  /**
   * Timeline系统保留的系统过滤器枚举，存储实体时自动添加到主过滤器中。
   * 这些key为系统保留，用户不应该使用相同key定义自定义过滤器，避免冲突。
   * 过滤器key区分大小写。
   */
  @Private
  enum SystemFilter {
    /** 实体所有者过滤器，标识时间线实体的所属用户 */
    ENTITY_OWNER
  }

}