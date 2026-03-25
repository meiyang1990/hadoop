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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.util.List;

/**
 * 时间线服务UID编码转换器，用于为前端UI查询生成可解析的唯一标识符，支持不同层级实体的编码解码。
 */
enum TimelineUIDConverter {
  /**
   * 流UID编码，包含集群ID、用户ID、流名称三个部分。
   */
  FLOW_UID {
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getUserId() == null ||
          context.getFlowName() == null) {
        return null;
      }
      String[] flowNameTupleArr = {context.getClusterId(), context.getUserId(),
          context.getFlowName()};
      return joinAndEscapeUIDParts(flowNameTupleArr);
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> flowNameTupleList = splitUID(uId);
      // 必须包含3个部分：集群、用户、流名称
      if (flowNameTupleList.size() != 3) {
        return null;
      }
      return new TimelineReaderContext(flowNameTupleList.get(0),
          flowNameTupleList.get(1), flowNameTupleList.get(2), null,
          null, null, null);
    }
  },

  /**
   * 流运行UID编码，包含集群ID、用户ID、流名称、流运行ID四个部分。
   */
  FLOWRUN_UID{
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getUserId() == null ||
          context.getFlowName() == null || context.getFlowRunId() == null) {
        return null;
      }
      String[] flowRunTupleArr = {context.getClusterId(), context.getUserId(),
          context.getFlowName(), context.getFlowRunId().toString()};
      return joinAndEscapeUIDParts(flowRunTupleArr);
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> flowRunTupleList = splitUID(uId);
      // 必须包含4个部分：集群、用户、流名称、流运行ID
      if (flowRunTupleList.size() != 4) {
        return null;
      }
      return new TimelineReaderContext(flowRunTupleList.get(0),
          flowRunTupleList.get(1), flowRunTupleList.get(2),
          Long.parseLong(flowRunTupleList.get(3)), null, null, null);
    }
  },

  /**
   * 应用UID编码，支持两种格式：带流上下文（5段）和不带流上下文（2段）。
   */
  APPLICATION_UID{
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getAppId() == null) {
        return null;
      }
      if (context.getUserId() != null && context.getFlowName() != null &&
          context.getFlowRunId() != null) {
        // 存在流上下文信息，编码为5段格式
        String[] appTupleArr = {context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId().toString(),
            context.getAppId()};
        return joinAndEscapeUIDParts(appTupleArr);
      } else {
        // 不存在流上下文信息，仅编码集群和应用ID两段格式
        String[] appTupleArr = {context.getClusterId(), context.getAppId()};
        return joinAndEscapeUIDParts(appTupleArr);
      }
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> appTupleList = splitUID(uId);
      // 支持两种合法格式：5段（集群、用户、流名称、流运行ID、应用ID）或2段（集群、应用ID）
      if (appTupleList.size() == 5) {
        // 存在流上下文信息
        return new TimelineReaderContext(appTupleList.get(0),
            appTupleList.get(1), appTupleList.get(2),
            Long.parseLong(appTupleList.get(3)), appTupleList.get(4),
            null, null);
      } else if (appTupleList.size() == 2) {
        // 不存在流上下文信息
        return new TimelineReaderContext(appTupleList.get(0), null, null, null,
            appTupleList.get(1), null, null);
      } else {
        return null;
      }
    }
  },

  /**
   * 子应用实体UID编码，包含集群ID、代理用户、实体类型、实体前缀、实体ID五个部分。
   */
  SUB_APPLICATION_ENTITY_UID {
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getDoAsUser() == null
          || context.getEntityType() == null || context.getEntityId() == null) {
        return null;
      }
      String[] entityTupleArr = {context.getClusterId(), context.getDoAsUser(),
          context.getEntityType(), context.getEntityIdPrefix().toString(),
          context.getEntityId()};
      return joinAndEscapeUIDParts(entityTupleArr);
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> entityTupleList = splitUID(uId);
      if (entityTupleList.size() == 5) {
        // 解析子应用实体上下文
        return new TimelineReaderContext(entityTupleList.get(0), null, null,
            null, null, entityTupleList.get(2),
            Long.parseLong(entityTupleList.get(3)), entityTupleList.get(4),
            entityTupleList.get(1));
      }
      return null;
    }
  },

  /**
   * 通用实体UID编码，支持两种格式：带流上下文（8段）和不带流上下文（5段）。
   */
  GENERIC_ENTITY_UID {
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getAppId() == null ||
          context.getEntityType() == null || context.getEntityId() == null) {
        return null;
      }
      if (context.getUserId() != null && context.getFlowName() != null &&
          context.getFlowRunId() != null) {
        // 存在流上下文信息，编码为8段格式
        String[] entityTupleArr = {context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId().toString(),
            context.getAppId(), context.getEntityType(),
            context.getEntityIdPrefix().toString(), context.getEntityId() };
        return joinAndEscapeUIDParts(entityTupleArr);
      } else {
        // 不存在流上下文信息，编码为5段格式
        String[] entityTupleArr = {context.getClusterId(), context.getAppId(),
            context.getEntityType(), context.getEntityIdPrefix().toString(),
            context.getEntityId() };
        return joinAndEscapeUIDParts(entityTupleArr);
      }
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> entityTupleList = splitUID(uId);
      // 支持两种合法格式：8段（带流上下文）或5段（不带流上下文）
      if (entityTupleList.size() == 8) {
        // 存在流上下文信息
        return new TimelineReaderContext(entityTupleList.get(0),
            entityTupleList.get(1), entityTupleList.get(2),
            Long.parseLong(entityTupleList.get(3)), entityTupleList.get(4),
            entityTupleList.get(5), Long.parseLong(entityTupleList.get(6)),
            entityTupleList.get(7));
      } else if (entityTupleList.size() == 5) {
        // 不存在流上下文信息
        return new TimelineReaderContext(entityTupleList.get(0), null, null,
            null, entityTupleList.get(1), entityTupleList.get(2),
            Long.parseLong(entityTupleList.get(3)), entityTupleList.get(4));
      } else {
        return null;
      }
    }
  };

  /**
   * 调用工具类拆分UID，使用默认分隔符和转义符处理。
   * @param uid 待拆分的UID字符串
   * @return 拆分后的UID各部分列表
   * @throws IllegalArgumentException 如果UID转义格式不正确
   */
  private static List<String> splitUID(String uid)
      throws IllegalArgumentException {
    return TimelineReaderUtils.split(uid);
  }

  /**
   * 调用工具类拼接并转义UID各部分，处理包含分隔符和转义符的情况。
   * @param parts 待拼接的UID各部分数组
   * @return 拼接转义完成的UID字符串，任意部分为null则返回null
   */
  private static String joinAndEscapeUIDParts(String[] parts) {
    return TimelineReaderUtils.joinAndEscapeStrings(parts);
  }

  /**
   * 根据上下文编码生成UID字符串。
   * @param context 时间线读取上下文
   * @return 编码后的UID字符串，必要信息缺失则返回null
   */
  abstract String encodeUID(TimelineReaderContext context);

  /**
   * 解码UID字符串生成时间线读取上下文。
   * @param uId 待解码的UID字符串
   * @return 解码得到的读取上下文，格式错误则返回null
   * @throws Exception 解码过程中发生异常
   */
  abstract TimelineReaderContext decodeUID(String uId) throws Exception;
}