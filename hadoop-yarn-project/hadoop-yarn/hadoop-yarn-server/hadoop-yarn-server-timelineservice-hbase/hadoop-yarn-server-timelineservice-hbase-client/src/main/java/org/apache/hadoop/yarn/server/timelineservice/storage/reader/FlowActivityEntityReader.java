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

package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityTableRW;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * 流活动实体读取器，负责从HBase流活动表读取时间线流活动实体数据。
 * 用于YARN时间线服务中查询指定条件下的流活动记录，支持按集群、时间范围、分页查询。
 */
class FlowActivityEntityReader extends TimelineEntityReader {
  /**
   * 流活动表读写操作实例，全局共享单例。
   */
  private static final FlowActivityTableRW FLOW_ACTIVITY_TABLE =
      new FlowActivityTableRW();

  /**
   * Long类型键转换器，用于在存储格式和Java类型之间转换键分量。
   */
  private final KeyConverter<Long> longKeyConverter = new LongKeyConverter();


  /**
   * 构造函数，传入上下文、过滤器和需要检索的数据配置。
   * @param ctxt 时间线读取器上下文，包含集群等信息
   * @param entityFilters 实体过滤条件
   * @param toRetrieve 需要检索的数据范围配置
   */
  FlowActivityEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve);
  }

  /**
   * 构造函数，仅传入上下文和需要检索的数据配置。
   * @param ctxt 时间线读取器上下文，包含集群等信息
   * @param toRetrieve 需要检索的数据范围配置
   */
  FlowActivityEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * 获取当前读取器操作的HBase表读写对象。
   */
  @Override
  protected BaseTableRW<?> getTable() {
    return FLOW_ACTIVITY_TABLE;
  }

  /**
   * 验证请求参数合法性，确保clusterId不为空。
   */
  @Override
  protected void validateParams() {
    String clusterId = getContext().getClusterId();
    if (clusterId == null) {
      throw new NullPointerException("clusterId shouldn't be null");
    }
  }

  /**
   * 补充查询参数，初始化过滤器（如果为空）。
   */
  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    createFiltersIfNull();
  }

  /**
   * 根据过滤条件构造HBase过滤器列表，该类暂不基于实体过滤条件构造过滤器。
   */
  @Override
  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    return null;
  }

  /**
   * 根据需要返回的字段构造HBase过滤器列表，该类暂不基于字段构造过滤器。
   */
  @Override
  protected FilterList constructFilterListBasedOnFields(
      Set<String> cfsInFields) {
    return null;
  }

  /**
   * 查询单个实体，不支持单实体查询。
   */
  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    throw new UnsupportedOperationException(
        "we don't support a single entity query");
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException {
    // 创建HBase扫描对象
    Scan scan = new Scan();
    // 从上下文获取集群ID
    String clusterId = getContext().getClusterId();
    // 如果没有起始ID且未指定时间范围，返回全部符合前缀的记录
    if (getFilters().getFromId() == null
        && getFilters().getCreatedTimeBegin() == 0L
        && getFilters().getCreatedTimeEnd() == Long.MAX_VALUE) {
       // 设置行前缀过滤，只查询该集群下的所有流活动记录
      scan.setRowPrefixFilter(new FlowActivityRowKeyPrefix(clusterId)
          .getRowKeyPrefix());
    } else if (getFilters().getFromId() != null) {
      // 从起始ID开始查询
      FlowActivityRowKey key = null;
      try {
        // 从字符串解析起始行键
        key =
            FlowActivityRowKey.parseRowKeyFromString(getFilters().getFromId());
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Invalid filter fromid is provided.");
      }
      // 校验起始ID所属集群与当前查询集群一致
      if (!clusterId.equals(key.getClusterId())) {
        throw new BadRequestException(
            "fromid doesn't belong to clusterId=" + clusterId);
      }
      // 设置扫描起始行
      scan.withStartRow(key.getRowKey());
      // 设置扫描结束行，限制时间范围不超过指定的开始时间
      scan.withStopRow(
          new FlowActivityRowKeyPrefix(clusterId,
              (getFilters().getCreatedTimeBegin() <= 0 ? 0
                  : (getFilters().getCreatedTimeBegin() - 1)))
                      .getRowKeyPrefix());
    } else {
      // 根据时间范围设置扫描区间，降序扫描指定时间窗口内的记录
      scan.withStartRow(new FlowActivityRowKeyPrefix(clusterId, getFilters()
          .getCreatedTimeEnd()).getRowKeyPrefix());
      scan.withStopRow(new FlowActivityRowKeyPrefix(clusterId, (getFilters()
          .getCreatedTimeBegin() <= 0 ? 0
          : (getFilters().getCreatedTimeBegin() - 1))).getRowKeyPrefix());
    }
    // 使用PageFilter限制返回结果数量，虽然HBase可能返回超过限制的结果，遍历过程中会截断
    scan.setFilter(new PageFilter(getFilters().getLimit()));
    // 从流活动表获取结果扫描器返回
    return getTable().getResultScanner(hbaseConf, conn, scan);
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    // 从HBase结果中解析出行键
    FlowActivityRowKey rowKey = FlowActivityRowKey.parseRowKey(result.getRow());

    // 提取行键中的时间、用户、流名称信息
    Long time = rowKey.getDayTimestamp();
    String user = rowKey.getUserId();
    String flowName = rowKey.getFlowName();

    // 构造流活动实体对象
    FlowActivityEntity flowActivity = new FlowActivityEntity(
        getContext().getClusterId(), time, user, flowName);
    // 设置实体ID
    flowActivity.setId(flowActivity.getId());
    // 读取当前流当日关联的所有流运行ID与版本信息
    Map<Long, Object> runIdsMap = ColumnRWHelper.readResults(result,
        FlowActivityColumnPrefix.RUN_ID, longKeyConverter);
    // 遍历所有流运行，转换为FlowRunEntity添加到流活动中
    for (Map.Entry<Long, Object> e : runIdsMap.entrySet()) {
      Long runId = e.getKey();
      String version = (String)e.getValue();
      FlowRunEntity flowRun = new FlowRunEntity();
      flowRun.setUser(user);
      flowRun.setName(flowName);
      flowRun.setRunId(runId);
      flowRun.setVersion(version);
      // 设置流运行实体ID
      flowRun.setId(flowRun.getId());
      flowActivity.addFlowRun(flowRun);
    }
    // 将行键字符串存入信息，用于分页查询的起始标记
    flowActivity.getInfo().put(TimelineReaderUtils.FROMID_KEY,
        rowKey.getRowKeyAsString());
    return flowActivity;
  }
}