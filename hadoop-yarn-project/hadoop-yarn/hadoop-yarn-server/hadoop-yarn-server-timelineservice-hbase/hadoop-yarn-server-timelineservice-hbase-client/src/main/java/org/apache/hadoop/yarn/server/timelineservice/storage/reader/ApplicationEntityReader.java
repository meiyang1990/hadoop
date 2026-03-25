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
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * 应用表存储的时间线实体读取器，负责从HBase应用表读取应用实体数据。
 */
class ApplicationEntityReader extends GenericEntityReader {
  private static final ApplicationTableRW APPLICATION_TABLE =
      new ApplicationTableRW();

  ApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve);
  }

  ApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * 获取应用表读写处理对象。
   */
  protected BaseTableRW<?> getTable() {
    return APPLICATION_TABLE;
  }

  /**
   * 仅在读取多个实体时调用，根据过滤条件构造HBase过滤器列表。
   */
  @Override
  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    TimelineEntityFilters filters = getFilters();
    FilterList listBasedOnFilters = new FilterList();
    // 按创建时间范围生成过滤条件并添加到过滤器列表
    long createdTimeBegin = filters.getCreatedTimeBegin();
    long createdTimeEnd = filters.getCreatedTimeEnd();
    if (createdTimeBegin != 0 || createdTimeEnd != Long.MAX_VALUE) {
      listBasedOnFilters.addFilter(
          TimelineFilterUtils.createSingleColValueFiltersByRange(
          ApplicationColumn.CREATED_TIME, createdTimeBegin, createdTimeEnd));
    }
    // 按指标过滤条件生成过滤器并添加
    TimelineFilterList metricFilters = filters.getMetricFilters();
    if (metricFilters != null && !metricFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(
          TimelineFilterUtils.createHBaseFilterList(
              ApplicationColumnPrefix.METRIC, metricFilters));
    }
    // 按配置过滤条件生成过滤器并添加
    TimelineFilterList configFilters = filters.getConfigFilters();
    if (configFilters != null && !configFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(
          TimelineFilterUtils.createHBaseFilterList(
              ApplicationColumnPrefix.CONFIG, configFilters));
    }
    // 按基本信息过滤条件生成过滤器并添加
    TimelineFilterList infoFilters = filters.getInfoFilters();
    if (infoFilters != null && !infoFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(
          TimelineFilterUtils.createHBaseFilterList(
              ApplicationColumnPrefix.INFO, infoFilters));
    }
    return listBasedOnFilters;
  }

  /**
   * 为应用表的固定列添加列限定符过滤条件。
   * @param list 要添加过滤器的列表
   */
  @Override
  protected void updateFixedColumns(FilterList list) {
    for (ApplicationColumn column : ApplicationColumn.values()) {
      list.addFilter(new QualifierFilter(CompareOp.EQUAL,
          new BinaryComparator(column.getColumnQualifierBytes())));
    }
  }

  /**
   * 创建info列族的列过滤列表，只返回指定需要的列。
   * @return 过滤列表
   * @throws IOException 创建过滤器时发生异常
   */
  private FilterList createFilterListForColsOfInfoFamily()
      throws IOException {
    FilterList infoFamilyColsFilter = new FilterList(Operator.MUST_PASS_ONE);
    // 添加应用表固定列的过滤条件
    updateFixedColumns(infoFamilyColsFilter);
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // 如果需要获取INFO字段，添加INFO列前缀的过滤条件
    if (hasField(fieldsToRetrieve, Field.INFO)) {
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, ApplicationColumnPrefix.INFO));
    }
    TimelineFilterList relatesTo = getFilters().getRelatesTo();
    if (hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      // 如果需要获取RELATES_TO字段，添加RELATES_TO列前缀的过滤条件
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, ApplicationColumnPrefix.RELATES_TO));
    } else if (relatesTo != null && !relatesTo.getFilterList().isEmpty()) {
      // 即使不需要返回RELATES_TO，若存在RELATES_TO过滤条件，仍需获取对应列用于服务端过滤
      Set<String> relatesToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(relatesTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          ApplicationColumnPrefix.RELATES_TO, relatesToCols));
    }
    TimelineFilterList isRelatedTo = getFilters().getIsRelatedTo();
    if (hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      // 如果需要获取IS_RELATED_TO字段，添加IS_RELATED_TO列前缀的过滤条件
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, ApplicationColumnPrefix.IS_RELATED_TO));
    } else if (isRelatedTo != null && !isRelatedTo.getFilterList().isEmpty()) {
      // 即使不需要返回IS_RELATED_TO，若存在IS_RELATED_TO过滤条件，仍需获取对应列用于服务端过滤
      Set<String> isRelatedToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(isRelatedTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          ApplicationColumnPrefix.IS_RELATED_TO, isRelatedToCols));
    }
    TimelineFilterList eventFilters = getFilters().getEventFilters();
    if (hasField(fieldsToRetrieve, Field.EVENTS)) {
      // 如果需要获取EVENTS字段，添加EVENT列前缀的过滤条件
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, ApplicationColumnPrefix.EVENT));
    } else if (eventFilters != null && !eventFilters.getFilterList().isEmpty()){
      // 即使不需要返回EVENTS，若存在EVENT过滤条件，仍需获取对应列用于服务端过滤
      Set<String> eventCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(eventFilters);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          ApplicationColumnPrefix.EVENT, eventCols));
    }
    return infoFamilyColsFilter;
  }

  /**
   * 根据需要获取的字段，排除info列族中不需要的列前缀。
   * @param infoColFamilyList info列族的过滤列表
   */
  private void excludeFieldsFromInfoColFamily(FilterList infoColFamilyList) {
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // 不需要事件，排除事件列前缀
    if (!hasField(fieldsToRetrieve, Field.EVENTS)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.NOT_EQUAL, ApplicationColumnPrefix.EVENT));
    }
    // 不需要基本信息，排除info列前缀
    if (!hasField(fieldsToRetrieve, Field.INFO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.NOT_EQUAL, ApplicationColumnPrefix.INFO));
    }
    // 不需要isRelatedTo，排除isRelatedTo列前缀
    if (!hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.NOT_EQUAL, ApplicationColumnPrefix.IS_RELATED_TO));
    }
    // 不需要relatesTo，排除relatesTo列前缀
    if (!hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.NOT_EQUAL, ApplicationColumnPrefix.RELATES_TO));
    }
  }

  /**
   * 根据需要获取的配置和指标，更新过滤列表。
   * @param listBasedOnFields 基于字段的过滤列表
   * @param cfsInFields 存储需要查询的列族名
   * @throws IOException 创建过滤器时发生异常
   */
  private void updateFilterForConfsAndMetricsToRetrieve(
      FilterList listBasedOnFields, Set<String> cfsInFields)
      throws IOException {
    TimelineDataToRetrieve dataToRetrieve = getDataToRetrieve();
    // 如果指定了需要获取的配置，添加对应过滤条件并标记列族需要查询
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.CONFIGS)) {
      listBasedOnFields.addFilter(TimelineFilterUtils.
          createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getConfsToRetrieve(),
              ApplicationColumnFamily.CONFIGS, ApplicationColumnPrefix.CONFIG));
      cfsInFields.add(
          Bytes.toString(ApplicationColumnFamily.CONFIGS.getBytes()));
    }

    // 如果指定了需要获取的指标，添加对应过滤条件并标记列族需要查询
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.METRICS)) {
      listBasedOnFields.addFilter(TimelineFilterUtils.
          createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getMetricsToRetrieve(),
              ApplicationColumnFamily.METRICS, ApplicationColumnPrefix.METRIC));
      cfsInFields.add(
          Bytes.toString(ApplicationColumnFamily.METRICS.getBytes()));
    }
  }

  @Override
  protected FilterList constructFilterListBasedOnFields(Set<String> cfsInFields)
      throws IOException {
    if (!needCreateFilterListBasedOnFields()) {
      // 获取所有列，不需要过滤
      return null;
    }
    FilterList listBasedOnFields = new FilterList(Operator.MUST_PASS_ONE);
    FilterList infoColFamilyList = new FilterList();
    // 默认匹配INFO列族
    FamilyFilter infoColumnFamily =
        new FamilyFilter(CompareOp.EQUAL,
            new BinaryComparator(ApplicationColumnFamily.INFO.getBytes()));
    infoColFamilyList.addFilter(infoColumnFamily);
    if (!isSingleEntityRead() && fetchPartialColsFromInfoFamily()) {
      // 仅从info列族获取部分列，创建对应列过滤列表
      infoColFamilyList.addFilter(createFilterListForColsOfInfoFamily());
    } else {
      // 排除info列族中不需要的列前缀
      excludeFieldsFromInfoColFamily(infoColFamilyList);
    }
    listBasedOnFields.addFilter(infoColFamilyList);
    cfsInFields.add(Bytes.toString(ApplicationColumnFamily.INFO.getBytes()));

    updateFilterForConfsAndMetricsToRetrieve(listBasedOnFields, cfsInFields);
    return listBasedOnFields;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    TimelineReaderContext context = getContext();
    // 构造单个应用的行键
    ApplicationRowKey applicationRowKey =
        new ApplicationRowKey(context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId(), context.getAppId());
    byte[] rowKey = applicationRowKey.getRowKey();
    Get get = new Get(rowKey);
    // 设置指标时间范围
    setMetricsTimeRange(get);
    // 设置指标最多返回版本数
    get.setMaxVersions(getDataToRetrieve().getMetricsLimit());
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      get.setFilter(filterList);
    }
    return getTable().getResult(hbaseConf, conn, get);
  }

  @Override
  protected void validateParams() {
    if (getContext() == null) {
      throw new NullPointerException("context shouldn't be null");
    }
    if (getDataToRetrieve() == null) {
      throw new NullPointerException("data to retrieve shouldn't be null");
    }
    if (getContext().getClusterId() == null) {
      throw new NullPointerException("clusterId shouldn't be null");
    }
    if (getContext().getEntityType() == null) {
      throw new NullPointerException("entityType shouldn't be null");
    }
    if (isSingleEntityRead()) {
      if (getContext().getAppId() == null) {
        throw new NullPointerException("appId shouldn't be null");
      }
    } else {
      if (getContext().getUserId() == null) {
        throw new NullPointerException("userId shouldn't be null");
      }
      if (getContext().getFlowName() == null) {
        throw new NullPointerException("flowName shouldn't be null");
      }
    }
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    if (isSingleEntityRead()) {
      // 从AppToFlow表补全流上下文信息
      defaultAugmentParams(hbaseConf, conn);
    }
    // 如果指定了需要获取的配置/指标，自动将对应字段添加到返回字段列表
    getDataToRetrieve().addFieldsBasedOnConfsAndMetricsToRetrieve();
    if (!isSingleEntityRead()) {
      // 如果过滤条件为空，创建默认过滤对象
      createFiltersIfNull();
    }
  }

  /**
   * 为查询设置指标值的时间范围。
   * @param query HBase查询对象
   */
  private void setMetricsTimeRange(Query query) {
    // Set time range for metric values.
    HBaseTimelineStorageUtils.setMetricsTimeRange(
        query, ApplicationColumnFamily.METRICS.getBytes(),
        getDataToRetrieve().getMetricsTimeBegin(),
        getDataToRetrieve().getMetricsTimeEnd());
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException {
    Scan scan = new Scan();
    TimelineReaderContext context = getContext();
    RowKeyPrefix<ApplicationRowKey> applicationRowKeyPrefix = null;

    // 未指定起始实体ID，默认从行前缀开头扫描
    if (getFilters().getFromId() == null) {