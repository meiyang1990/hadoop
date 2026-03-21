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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 承载离线聚合信息的数据类，供存储层实现使用。
 * 当前预定义了流级别和用户级别两种离线聚合实例，存储层实现可根据该对象动态决定处理行为。
 */
public final class OfflineAggregationInfo {
  /**
   * 默认流级别聚合表名。
   */
  @VisibleForTesting
  public static final String FLOW_AGGREGATION_TABLE_NAME
      = "yarn_timeline_flow_aggregation";
  /**
   * 默认用户级别聚合表名。
   */
  public static final String USER_AGGREGATION_TABLE_NAME
      = "yarn_timeline_user_aggregation";

  // 主键列表不会影响表创建逻辑
  private static final String[] FLOW_AGGREGATION_PK_LIST = {
      "user", "cluster", "flow_name"
  };
  private static final String[] USER_AGGREGATION_PK_LIST = {
      "user", "cluster"
  };

  private final String tableName;
  private final String[] primaryKeyList;
  private final PrimaryKeyStringSetter primaryKeyStringSetter;

  private OfflineAggregationInfo(String table, String[] pkList,
      PrimaryKeyStringSetter formatter) {
    tableName = table;
    primaryKeyList = pkList;
    primaryKeyStringSetter = formatter;
  }

  /**
   * 主键值设置到预编译SQL语句的函数接口。
   */
  private interface PrimaryKeyStringSetter {
    /**
     * 将主键值设置到预编译语句中。
     * @param ps 预编译SQL语句
     * @param context 时间线收集器上下文，含聚合维度信息
     * @param extraInfo 额外信息数组
     * @param startPos 开始设置参数的起始位置
     * @return 下一个可用参数位置
     * @throws SQLException SQL异常
     */
    int setValues(PreparedStatement ps, TimelineCollectorContext context,
        String[] extraInfo, int startPos) throws SQLException;
  }

  /**
   * 获取聚合表名。
   * @return 聚合表名
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * 获取主键列名列表拷贝。
   * @return 主键列名数组拷贝
   */
  public String[] getPrimaryKeyList() {
    return primaryKeyList.clone();
  }

  /**
   * 将主键值设置到预编译SQL语句中。
   * @param ps 预编译SQL语句
   * @param context 时间线收集器上下文
   * @param extraInfo 额外信息
   * @param startPos 起始参数位置
   * @return 下一个可用参数位置
   * @throws SQLException SQL异常
   */
  public int setStringsForPrimaryKey(PreparedStatement ps,
      TimelineCollectorContext context, String[] extraInfo, int startPos)
      throws SQLException {
    return primaryKeyStringSetter.setValues(ps, context, extraInfo, startPos);
  }

  /**
   * 流级别离线聚合实例，按用户、集群、流名称聚合。
   */
  public static final OfflineAggregationInfo FLOW_AGGREGATION =
      new OfflineAggregationInfo(FLOW_AGGREGATION_TABLE_NAME,
          FLOW_AGGREGATION_PK_LIST,
          new PrimaryKeyStringSetter() {
          @Override
          public int setValues(PreparedStatement ps,
              TimelineCollectorContext context, String[] extraInfo,
              int startPos) throws SQLException {
            int idx = startPos;
            // 设置用户ID
            ps.setString(idx++, context.getUserId());
            // 设置集群ID
            ps.setString(idx++, context.getClusterId());
            // 设置流名称
            ps.setString(idx++, context.getFlowName());
            return idx;
          }
        });

  /**
   * 用户级别离线聚合实例，按用户、集群聚合。
   */
  public static final OfflineAggregationInfo USER_AGGREGATION =
      new OfflineAggregationInfo(USER_AGGREGATION_TABLE_NAME,
          USER_AGGREGATION_PK_LIST,
          new PrimaryKeyStringSetter() {
          @Override
          public int setValues(PreparedStatement ps,
              TimelineCollectorContext context, String[] extraInfo,
              int startPos) throws SQLException {
            int idx = startPos;
            // 设置用户ID
            ps.setString(idx++, context.getUserId());
            // 设置集群ID
            ps.setString(idx++, context.getClusterId());
            return idx;
          }
        });
}