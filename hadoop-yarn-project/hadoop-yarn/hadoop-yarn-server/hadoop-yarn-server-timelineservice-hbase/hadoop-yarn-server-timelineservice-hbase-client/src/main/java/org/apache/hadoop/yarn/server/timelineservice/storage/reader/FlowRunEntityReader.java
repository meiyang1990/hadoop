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
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTableRW;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * 存储在Flow Run表中的流程运行实体时间线读取器，负责从HBase Flow Run表查询并解析流程运行实体数据。
 */
class FlowRunEntityReader extends TimelineEntityReader {
  private static final FlowRunTableRW FLOW_RUN_TABLE = new FlowRunTableRW();

  FlowRunEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve);
  }

  FlowRunEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * 获取Flow Run表读写器实例。
   */
  @Override
  protected BaseTableRW<?> getTable() {
    return FLOW_RUN_TABLE;
  }

  @Override
  protected void validateParams() {
    // 校验上下文对象非空
    if (getContext() == null) {
      throw new NullPointerException("context shouldn't be null");
    }
    // 校验待获取数据描述对象非空
    if (getDataToRetrieve() == null) {
      throw new NullPointerException("data to retrieve shouldn't be null");
    }
    // 校验集群ID非空
    if (getContext().getClusterId() == null) {
      throw new NullPointerException("clusterId shouldn't be null");
    }
    // 校验用户ID非空
    if (getContext().getUserId() == null) {
      throw new NullPointerException("userId shouldn't be null");
    }
    // 校验流程名称非空
    if (getContext().getFlowName() == null) {
      throw new NullPointerException("flowName shouldn't be null");
    }
    // 单实体查询时，校验流程运行ID非空
    if (isSingleEntityRead()) {
      if (getContext().getFlowRunId() == null) {
        throw new NullPointerException("flowRunId shouldn't be null");
      }
    }
    // 批量查询时校验字段合法性，批量查询只允许获取全部字段或指标
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    if (!isSingleEntityRead() && fieldsToRetrieve != null) {
      for (Field field : fieldsToRetrieve) {
        if (field != Field.ALL && field != Field.METRICS) {
          throw new BadRequestException("Invalid field " + field
              + " specified while querying flow runs.");
        }
      }
    }
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn) {
    // 根据配置和待获取指标列表，补充需要获取的字段
    getDataToRetrieve().addFieldsBasedOnConfsAndMetricsToRetrieve();
    // 批量查询时初始化过滤器
    if (!isSingleEntityRead()) {
      createFiltersIfNull();
    }
  }

  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    FilterList listBasedOnFilters = new FilterList();
    // 根据创建时间范围添加过滤条件
    Long createdTimeBegin = getFilters().getCreatedTimeBegin();
    Long createdTimeEnd = getFilters().getCreatedTimeEnd();
    if (createdTimeBegin != 0 || createdTimeEnd != Long.MAX_VALUE) {
      listBasedOnFilters.addFilter(TimelineFilterUtils
          .createSingleColValueFiltersByRange(FlowRunColumn.MIN_START_TIME,
              createdTimeBegin, createdTimeEnd));
    }
    // 根据指标过滤器添加过滤条件
    TimelineFilterList metricFilters = getFilters().getMetricFilters();
    if (metricFilters != null && !metricFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils.createHBaseFilterList(
          FlowRunColumnPrefix.METRIC, metricFilters));
    }
    return listBasedOnFilters;
  }

  /**
   * 为Flow Run表固定列添加Qualifier过滤条件，只保留预设的固定列。
   *
   * @return 添加了固定列过滤条件的过滤器列表
   */
  private FilterList updateFixedColumns() {
    FilterList columnsList = new FilterList(Operator.MUST_PASS_ONE);
    for (FlowRunColumn column : FlowRunColumn.values()) {
      columnsList.addFilter(new QualifierFilter(CompareOp.EQUAL,
          new BinaryComparator(column.getColumnQualifierBytes())));
    }
    return columnsList;
  }

  @Override
  protected FilterList constructFilterListBasedOnFields(
      Set<String> cfsInFields) throws IOException {
    FilterList list = new FilterList(Operator.MUST_PASS_ONE);
    // 构造INFO列簇过滤条件，默认只读取INFO列簇
    FamilyFilter infoColumnFamily =
        new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(
            FlowRunColumnFamily.INFO.getBytes()));
    TimelineDataToRetrieve dataToRetrieve = getDataToRetrieve();
    // 批量查询且不需要获取指标时，过滤掉所有指标列
    if (!isSingleEntityRead()
        && !hasField(dataToRetrieve.getFieldsToRetrieve(), Field.METRICS)) {
      FilterList infoColFamilyList = new FilterList(Operator.MUST_PASS_ONE);
      infoColFamilyList.addFilter(infoColumnFamily);
      cfsInFields.add(Bytes.toString(FlowRunColumnFamily.INFO.getBytes()));
      infoColFamilyList.addFilter(new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(FlowRunColumnPrefix.METRIC
              .getColumnPrefixBytes(""))));
      list.addFilter(infoColFamilyList);
    } else {
      // 需要获取指标时，根据指定的指标列表过滤
      TimelineFilterList metricsToRetrieve =
          dataToRetrieve.getMetricsToRetrieve();
      if (metricsToRetrieve != null
          && !metricsToRetrieve.getFilterList().isEmpty()) {
        FilterList infoColFamilyList = new FilterList();
        infoColFamilyList.addFilter(infoColumnFamily);
        cfsInFields.add(Bytes.toString(FlowRunColumnFamily.INFO.getBytes()));
        // 添加固定列过滤条件
        FilterList columnsList = updateFixedColumns();
        // 添加指定指标的过滤条件
        columnsList.addFilter(TimelineFilterUtils.createHBaseFilterList(
            FlowRunColumnPrefix.METRIC, metricsToRetrieve));
        infoColFamilyList.addFilter(columnsList);
        list.addFilter(infoColFamilyList);
      }
    }
    return list;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    TimelineReaderContext context = getContext();
    // 构造单实体查询行键
    FlowRunRowKey flowRunRowKey =
        new FlowRunRowKey(context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId());
    byte[] rowKey = flowRunRowKey.getRowKey();
    Get get = new Get(rowKey);
    // 获取所有版本的数据
    get.setMaxVersions(Integer.MAX_VALUE);
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      get.setFilter(filterList);
    }
    // 执行Get查询返回结果
    return getTable().getResult(hbaseConf, conn, get);
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    Scan scan = new Scan();
    TimelineReaderContext context = getContext();
    RowKeyPrefix<FlowRunRowKey> flowRunRowKeyPrefix = null;
    // 未指定起始ID时，按行键前缀扫描所有符合条件的流程运行
    if (getFilters().getFromId() == null) {
      flowRunRowKeyPrefix = new FlowRunRowKeyPrefix(context.getClusterId(),
          context.getUserId(), context.getFlowName());
      scan.setRowPrefixFilter(flowRunRowKeyPrefix.getRowKeyPrefix());
    } else {
      // 解析起始ID行键
      FlowRunRowKey flowRunRowKey = null;
      try {
        flowRunRowKey =
            FlowRunRowKey.parseRowKeyFromString(getFilters().getFromId());
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Invalid filter fromid is provided.");
      }
      // 校验起始ID所属集群与查询集群一致
      if (!context.getClusterId().equals(flowRunRowKey.getClusterId())) {
        throw new BadRequestException(
            "fromid doesn't belong to clusterId=" + context.getClusterId());
      }
      // 设置扫描起始行
      scan.withStartRow(flowRunRowKey.getRowKey());

      // 构造同前缀下的结束行键
      flowRunRowKeyPrefix = new FlowRunRowKeyPrefix(context.getClusterId(),
          context.getUserId(), context.getFlowName());

      // 设置扫描结束行，保证只扫描当前前缀范围内的行
      scan.withStopRow(
          HBaseTimelineStorageUtils.calculateTheClosestNextRowKeyForPrefix(
              flowRunRowKeyPrefix.getRowKeyPrefix()));
    }

    FilterList newList = new FilterList();
    // 添加分页过滤器，限制返回结果数量
    newList.addFilter(new PageFilter(getFilters().getLimit()));
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      newList.addFilter(filterList);
    }
    scan.setFilter(newList);
    // 获取所有版本的数据
    scan.setMaxVersions(Integer.MAX_VALUE);
    // 执行Scan查询返回结果扫描器
    return getTable().getResultScanner(hbaseConf, conn, scan);
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    FlowRunEntity flowRun = new FlowRunEntity();
    // 从结果行键解析Flow Run元数据
    FlowRunRowKey rowKey = FlowRunRowKey.parseRowKey(result.getRow());
    flowRun.setRunId(rowKey.getFlowRunId());
    flowRun.setUser(rowKey.getUserId());
    flowRun.setName(rowKey.getFlowName());

    // 读取并设置流程启动时间
    Long startTime = (Long) ColumnRWHelper.readResult(result,
        FlowRunColumn.MIN_START_TIME);
    if (startTime != null) {
      flowRun.setStartTime(startTime.longValue());
    }

    // 读取并设置流程最大结束时间
    Long endTime = (Long) ColumnRWHelper.readResult(result,
        FlowRunColumn.MAX_END_TIME);
    if (endTime != null) {
      flowRun.setMaxEndTime(endTime.longValue());
    }

    // 读取并设置流程版本
    String version = (String) ColumnRWHelper.readResult(result,
        FlowRunColumn.FLOW_VERSION);
    if (version != null) {
      flowRun.setVersion(version);
    }

    // 单实体查询或需要获取指标时，读取指标数据
    if (isSingleEntityRead()
        || hasField(getDataToRetrieve().getFieldsToRetrieve(), Field.METRICS)) {
      readMetrics(flowRun, result, FlowRunColumnPrefix.METRIC);
    }

    // 设置实体ID和分页起始键
    flowRun.setId(flowRun.getId());
    flowRun.getInfo().put(TimelineReaderUtils.FROMID_KEY,
        rowKey.getRowKeyAsString());
    return flowRun;
  }
}