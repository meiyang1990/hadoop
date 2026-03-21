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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationTableRW;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * 子应用时间线实体HBase读取器，负责从子应用表读取时间线实体数据
 * 继承自GenericEntityReader，实现针对子应用表的特定查询逻辑
 */
class SubApplicationEntityReader extends GenericEntityReader {
  private static final SubApplicationTableRW SUB_APPLICATION_TABLE =
      new SubApplicationTableRW();

  /**
   * 构造函数，带过滤条件和要获取的数据
   * @param ctxt 时间线读取器上下文
   * @param entityFilters 实体过滤条件
   * @param toRetrieve 要获取的数据描述
   */
  SubApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve);
  }

  /**
   * 构造函数，仅带要获取的数据，过滤条件留空
   * @param ctxt 时间线读取器上下文
   * @param toRetrieve 要获取的数据描述
   */
  SubApplicationEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * 获取子应用表读写操作对象
   * @return 子应用表RW对象
   */
  protected BaseTableRW<?> getTable() {
    return SUB_APPLICATION_TABLE;
  }

  @Override
  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    // 多实体读取中过滤条件不可能为null，会在augmentParams中初始化
    FilterList listBasedOnFilters = new FilterList();
    TimelineEntityFilters filters = getFilters();
    // 根据创建时间范围创建过滤器，添加到过滤器列表
    long createdTimeBegin = filters.getCreatedTimeBegin();
    long createdTimeEnd = filters.getCreatedTimeEnd();
    if (createdTimeBegin != 0 || createdTimeEnd != Long.MAX_VALUE) {
      listBasedOnFilters.addFilter(TimelineFilterUtils
          .createSingleColValueFiltersByRange(SubApplicationColumn.CREATED_TIME,
              createdTimeBegin, createdTimeEnd));
    }
    // 根据指标过滤条件创建过滤器，添加到过滤器列表
    TimelineFilterList metricFilters = filters.getMetricFilters();
    if (metricFilters != null && !metricFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils.createHBaseFilterList(
          SubApplicationColumnPrefix.METRIC, metricFilters));
    }
    // 根据配置过滤条件创建过滤器，添加到过滤器列表
    TimelineFilterList configFilters = filters.getConfigFilters();
    if (configFilters != null && !configFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils.createHBaseFilterList(
          SubApplicationColumnPrefix.CONFIG, configFilters));
    }
    // 根据信息过滤条件创建过滤器，添加到过滤器列表
    TimelineFilterList infoFilters = filters.getInfoFilters();
    if (infoFilters != null && !infoFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils
          .createHBaseFilterList(SubApplicationColumnPrefix.INFO, infoFilters));
    }
    return listBasedOnFilters;
  }

  /**
   * 为每个固定列添加列限定符过滤器到过滤器列表
   * @param list 要添加过滤器的过滤器列表
   */
  protected void updateFixedColumns(FilterList list) {
    for (SubApplicationColumn column : SubApplicationColumn.values()) {
      list.addFilter(new QualifierFilter(CompareOp.EQUAL,
          new BinaryComparator(column.getColumnQualifierBytes())));
    }
  }

  /**
   * 创建针对info列族的列级过滤列表，只返回需要的部分列
   * @return 过滤后的过滤器列表
   * @throws IOException 创建过滤器列表出错时抛出
   */
  private FilterList createFilterListForColsOfInfoFamily() throws IOException {
    FilterList infoFamilyColsFilter = new FilterList(Operator.MUST_PASS_ONE);
    // 添加固定列的过滤器
    updateFixedColumns(infoFamilyColsFilter);
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // 如果需要获取INFO字段，添加INFO列前缀的过滤器
    if (hasField(fieldsToRetrieve, Field.INFO)) {
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.EQUAL,
              SubApplicationColumnPrefix.INFO));
    }
    TimelineFilterList relatesTo = getFilters().getRelatesTo();
    if (hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      // 如果需要获取RELATES_TO字段，添加RELATES_TO列前缀的过滤器
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.EQUAL,
              SubApplicationColumnPrefix.RELATES_TO));
    } else if (relatesTo != null && !relatesTo.getFilterList().isEmpty()) {
      // 即使不需要返回RELATES_TO，为了过滤也需要获取指定列，后续在内存过滤
      Set<String> relatesToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(relatesTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          SubApplicationColumnPrefix.RELATES_TO, relatesToCols));
    }
    TimelineFilterList isRelatedTo = getFilters().getIsRelatedTo();
    if (hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      // 如果需要获取IS_RELATED_TO字段，添加IS_RELATED_TO列前缀的过滤器
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.EQUAL,
              SubApplicationColumnPrefix.IS_RELATED_TO));
    } else if (isRelatedTo != null && !isRelatedTo.getFilterList().isEmpty()) {
      // 即使不需要返回IS_RELATED_TO，为了过滤也需要获取指定列，后续在内存过滤
      Set<String> isRelatedToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(isRelatedTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          SubApplicationColumnPrefix.IS_RELATED_TO, isRelatedToCols));
    }
    TimelineFilterList eventFilters = getFilters().getEventFilters();
    if (hasField(fieldsToRetrieve, Field.EVENTS)) {
      // 如果需要获取EVENTS字段，添加EVENT列前缀的过滤器
      infoFamilyColsFilter.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.EQUAL,
              SubApplicationColumnPrefix.EVENT));
    } else if (eventFilters != null
        && !eventFilters.getFilterList().isEmpty()) {
      // 即使不需要返回EVENTS，为了过滤也需要获取指定列，后续在内存过滤
      Set<String> eventCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(eventFilters);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          SubApplicationColumnPrefix.EVENT, eventCols));
    }
    return infoFamilyColsFilter;
  }

  /**
   * 从info列族中排除不需要获取的列前缀，基于要获取的字段列表过滤
   * @param infoColFamilyList info列族的过滤器列表
   */
  private void excludeFieldsFromInfoColFamily(FilterList infoColFamilyList) {
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // 排除不需要的事件列
    if (!hasField(fieldsToRetrieve, Field.EVENTS)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              SubApplicationColumnPrefix.EVENT));
    }
    // 排除不需要的info列
    if (!hasField(fieldsToRetrieve, Field.INFO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              SubApplicationColumnPrefix.INFO));
    }
    // 排除不需要的is related to列
    if (!hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              SubApplicationColumnPrefix.IS_RELATED_TO));
    }
    // 排除不需要的relates to列
    if (!hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      infoColFamilyList.addFilter(
          TimelineFilterUtils.createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              SubApplicationColumnPrefix.RELATES_TO));
    }
  }

  /**
   * 根据要获取的配置和指标更新过滤器列表
   * @param listBasedOnFields 基于字段的过滤器列表
   * @param cfsInFields 需要返回的列族名集合
   * @throws IOException 创建过滤器出错时抛出
   */
  private void updateFilterForConfsAndMetricsToRetrieve(
      FilterList listBasedOnFields, Set<String> cfsInFields)
      throws IOException {
    TimelineDataToRetrieve dataToRetrieve = getDataToRetrieve();
    // 注意：如果指定了要获取的配置列表，即使没在字段中指定，也会自动添加CONFIGS字段
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.CONFIGS)) {
      // 创建配置列过滤器
      listBasedOnFields.addFilter(
          TimelineFilterUtils.createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getConfsToRetrieve(),
              SubApplicationColumnFamily.CONFIGS,
              SubApplicationColumnPrefix.CONFIG));
      cfsInFields.add(
          Bytes.toString(SubApplicationColumnFamily.CONFIGS.getBytes()));
    }

    // 注意：如果指定了要获取的指标列表，即使没在字段中指定，也会自动添加METRICS字段
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.METRICS)) {
      // 创建指标列过滤器
      listBasedOnFields.addFilter(
          TimelineFilterUtils.createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getMetricsToRetrieve(),
              SubApplicationColumnFamily.METRICS,
              SubApplicationColumnPrefix.METRIC));
      cfsInFields.add(
          Bytes.toString(SubApplicationColumnFamily.METRICS.getBytes()));
    }
  }

  @Override
  protected FilterList constructFilterListBasedOnFields(Set<String> cfsInFields)
      throws IOException {
    if (!needCreateFilterListBasedOnFields()) {
      // 获取所有列，不需要过滤器
      return null;
    }
    FilterList listBasedOnFields = new FilterList(Operator.MUST_PASS_ONE);
    FilterList infoColFamilyList = new FilterList();
    // 默认先获取整个info列族
    FamilyFilter infoColumnFamily = new FamilyFilter(CompareOp.EQUAL,
        new BinaryComparator(SubApplicationColumnFamily.INFO.getBytes()));
    infoColFamilyList.addFilter(infoColumnFamily);
    if (fetchPartialColsFromInfoFamily()) {
      // 只需要获取info列族中的部分列，创建部分列过滤列表
      infoColFamilyList.addFilter(createFilterListForColsOfInfoFamily());
    } else {
      // 排除info列族中不需要的列前缀
      excludeFieldsFromInfoColFamily(infoColFamilyList);
    }
    listBasedOnFields.addFilter(infoColFamilyList);
    cfsInFields.add(
        Bytes.toString(SubApplicationColumnFamily.INFO.getBytes()));
    updateFilterForConfsAndMetricsToRetrieve(listBasedOnFields, cfsInFields);
    return listBasedOnFields;
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
    if (getContext().getDoAsUser() == null) {
      throw new NullPointerException("DoAsUser shouldn't be null");
    }
    if (getContext().getEntityType() == null) {
      throw new NullPointerException("entityType shouldn't be null");
    }
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    // 根据配置和指标要获取列表自动添加对应字段
    getDataToRetrieve().addFieldsBasedOnConfsAndMetricsToRetrieve();
    // 如果过滤条件为空则创建空过滤条件
    createFiltersIfNull();
  }

  /**
   * 为查询设置指标时间范围过滤
   * @param query HBase查询对象
   */
  private void setMetricsTimeRange(Query query) {
    // 为指标值设置时间范围
    HBaseTimelineStorageUtils.setMetricsTimeRange(query,
        SubApplicationColumnFamily.METRICS.getBytes(),
        getDataToRetrieve().getMetricsTimeBegin(),
        getDataToRetrieve().getMetricsTimeEnd());
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {

    // 扫描表中属于同一个应用和类型的实体
    Scan scan = new Scan();
    TimelineReaderContext context = getContext();
    if (context.getDoAsUser() == null) {
      throw new BadRequestException("Invalid user!");
    }

    RowKeyPrefix<SubApplicationRowKey> subApplicationRowKeyPrefix = null;
    // 默认模式，从实体类型起始位置开始扫描
    if (getFilters() == null || getFilters().getFromId() == null) {
      subApplicationRowKeyPrefix = new SubApplicationRowKeyPrefix(
          context.getDoAsUser(), context.getClusterId(),
          context.getEntityType(), null, null, null);
      // 设置行前缀过滤，只扫描该前缀下的行
      scan.setRowPrefixFilter(subApplicationRowKeyPrefix.getRowKeyPrefix());
    } else { // 分页模式，从指定实体ID位置开始扫描
      SubApplicationRowKey entityRowKey = null;
      try {
        // 解析起始行键
        entityRowKey = SubApplicationRowKey
            .parseRowKeyFromString(getFilters().getFromId());
      } catch (IllegalArgumentException e) {
        throw new Bad