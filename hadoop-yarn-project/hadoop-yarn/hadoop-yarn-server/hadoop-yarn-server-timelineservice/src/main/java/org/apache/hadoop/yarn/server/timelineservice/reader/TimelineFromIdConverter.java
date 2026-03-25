// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.util.List;

/**
 * YARN时间线服务FROM_ID编解码器，负责将分页查询起始标识FROM_ID解析为时间线读取上下文
 * 支持不同类型实体的FROM_ID格式解析，用于分页查询场景
 */
enum TimelineFromIdConverter {

  /** 应用级FROM_ID解析实现 */
  APPLICATION_FROMID {
    @Override TimelineReaderContext decodeUID(String fromId) throws Exception {
      if (fromId == null) {
        return null;
      }

      // 拆分FROM_ID字符串为各个字段
      List<String> appTupleList = TimelineReaderUtils.split(fromId);
      // 校验字段数量是否符合应用表行键格式
      if (appTupleList == null || appTupleList.size() != 5) {
        throw new IllegalArgumentException(
            "Invalid row key for application table.");
      }

      // 构建对应时间线读取上下文
      return new TimelineReaderContext(appTupleList.get(0), appTupleList.get(1),
          appTupleList.get(2), Long.parseLong(appTupleList.get(3)),
          appTupleList.get(4), null, null);
    }
  },

  /** 子应用实体级FROM_ID解析实现 */
  SUB_APPLICATION_ENTITY_FROMID {
    @Override TimelineReaderContext decodeUID(String fromId) throws Exception {
      if (fromId == null) {
        return null;
      }
      // 拆分FROM_ID字符串为各个字段
      List<String> split = TimelineReaderUtils.split(fromId);
      // 校验字段数量是否符合子应用表行键格式
      if (split == null || split.size() != 6) {
        throw new IllegalArgumentException(
            "Invalid row key for sub app table.");
      }

      // 提取各个字段
      String subAppUserId = split.get(0);
      String clusterId = split.get(1);
      String entityType = split.get(2);
      Long entityIdPrefix = Long.valueOf(split.get(3));
      String entityId = split.get(4);
      String userId = split.get(5);
      // 构建对应时间线读取上下文
      return new TimelineReaderContext(clusterId, userId, null, null, null,
          entityType, entityIdPrefix, entityId, subAppUserId);
    }
  },

  /** 通用实体级FROM_ID解析实现 */
  GENERIC_ENTITY_FROMID {
    @Override TimelineReaderContext decodeUID(String fromId) throws Exception {
      if (fromId == null) {
        return null;
      }
      // 拆分FROM_ID字符串为各个字段
      List<String> split = TimelineReaderUtils.split(fromId);
      // 校验字段数量是否符合实体表行键格式
      if (split == null || split.size() != 8) {
        throw new IllegalArgumentException("Invalid row key for entity table.");
      }
      // 解析数值类型字段
      Long flowRunId = Long.valueOf(split.get(3));
      Long entityIdPrefix = Long.valueOf(split.get(6));
      // 构建对应时间线读取上下文
      return new TimelineReaderContext(split.get(0), split.get(1), split.get(2),
          flowRunId, split.get(4), split.get(5), entityIdPrefix, split.get(7));
    }
  };

  /**
   * 根据具体实体类型解析FROM_ID，生成时间线读取上下文
   *
   * @param fromId 分页查询起始标识FROM_ID
   * @return 解析成功返回时间线读取上下文，输入为null返回null
   * @throws Exception 解析格式错误时抛出异常
   */
  abstract TimelineReaderContext decodeUID(String fromId) throws Exception;
}