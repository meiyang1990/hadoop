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
import java.util.Iterator;
import java.util.Map;
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
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.StringKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTableRW;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * 存储在实体表中的通用时间线实体读取器，负责从HBase实体表读取时间线实体数据。
 */
class GenericEntityReader extends TimelineEntityReader {
  // 实体表读写对象单例
  private static final EntityTableRW ENTITY_TABLE = new EntityTableRW();

  /**
   * 字符串键转换器，用于字符串键在存储格式和Java格式之间转换。
   */
  private final KeyConverter<String> stringKeyConverter =
      new StringKeyConverter();

  /**
   * 构造函数，用于多实体读取场景。
   * @param ctxt 读取器上下文
   * @param entityFilters 实体过滤条件
   * @param toRetrieve 需要获取的数据描述
   */
  GenericEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve);
  }

  /**
   * 构造函数，用于单实体读取场景。
   * @param ctxt 读取器上下文
   * @param toRetrieve 需要获取的数据描述
   */
  GenericEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * 获取实体表读写对象。
   */
  protected BaseTableRW<?> getTable() {
    return ENTITY_TABLE;
  }

  @Override
  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    // 多实体读取场景下过滤条件不可能为空，会在augmentParams中补空
    FilterList listBasedOnFilters = new FilterList();
    TimelineEntityFilters filters = getFilters();
    // 根据创建时间范围创建过滤条件并添加到过滤列表
    long createdTimeBegin = filters.getCreatedTimeBegin();
    long createdTimeEnd = filters.getCreatedTimeEnd();
    if (createdTimeBegin != 0 || createdTimeEnd != Long.MAX_VALUE) {
      listBasedOnFilters.addFilter(TimelineFilterUtils
          .createSingleColValueFiltersByRange(EntityColumn.CREATED_TIME,
              createdTimeBegin, createdTimeEnd));
    }
    // 根据指标过滤条件创建过滤列表并添加到过滤列表
    TimelineFilterList metricFilters = filters.getMetricFilters();
    if (metricFilters != null && !metricFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils.createHBaseFilterList(
          EntityColumnPrefix.METRIC, metricFilters));
    }
    // 根据配置过滤条件创建过滤列表并添加到过滤列表
    TimelineFilterList configFilters = filters.getConfigFilters();
    if (configFilters != null && !configFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils.createHBaseFilterList(
          EntityColumnPrefix.CONFIG, configFilters));
    }
    // 根据信息过滤条件创建过滤列表并添加到过滤列表
    TimelineFilterList infoFilters = filters.getInfoFilters();
    if (infoFilters != null && !infoFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils.createHBaseFilterList(
          EntityColumnPrefix.INFO, infoFilters));
    }
    return listBasedOnFilters;
  }

  /**
   * 检查是否只需要获取部分事件列。
   * @param eventFilters 事件过滤条件
   * @param fieldsToRetrieve 需要获取的字段集合
   * @return 是否只需要获取部分事件列
   */
  protected boolean fetchPartialEventCols(TimelineFilterList eventFilters,
      EnumSet<Field> fieldsToRetrieve) {
    return (eventFilters != null && !eventFilters.getFilterList().isEmpty() &&
        !hasField(fieldsToRetrieve, Field.EVENTS));
  }

  /**
   * 检查是否只需要获取部分relates_to关联列。
   * @param relatesTo relates_to过滤条件
   * @param fieldsToRetrieve 需要获取的字段集合
   * @return 是否只需要获取部分relates_to关联列
   */
  protected boolean fetchPartialRelatesToCols(TimelineFilterList relatesTo,
      EnumSet<Field> fieldsToRetrieve) {
    return (relatesTo != null && !relatesTo.getFilterList().isEmpty() &&
        !hasField(fieldsToRetrieve, Field.RELATES_TO));
  }

  /**
   * 检查是否只需要获取部分is_related_to关联列。
   * @param isRelatedTo is_related_to过滤条件
   * @param fieldsToRetrieve 需要获取的字段集合
   * @return 是否只需要获取部分is_related_to关联列
   */
  private boolean fetchPartialIsRelatedToCols(TimelineFilterList isRelatedTo,
      EnumSet<Field> fieldsToRetrieve) {
    return (isRelatedTo != null && !isRelatedTo.getFilterList().isEmpty() &&
        !hasField(fieldsToRetrieve, Field.IS_RELATED_TO));
  }

  /**
   * 检查info列族是否只需要获取部分列。
   * @return 是否只需要获取info列族的部分列
   */
  protected boolean fetchPartialColsFromInfoFamily() {
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    TimelineEntityFilters filters = getFilters();
    return fetchPartialEventCols(filters.getEventFilters(), fieldsToRetrieve)
        || fetchPartialRelatesToCols(filters.getRelatesTo(), fieldsToRetrieve)
        || fetchPartialIsRelatedToCols(filters.getIsRelatedTo(),
            fieldsToRetrieve);
  }

  /**
   * 检查是否需要基于字段创建过滤列表。
   * @return 是否需要基于字段创建过滤列表
   */
  protected boolean needCreateFilterListBasedOnFields() {
    TimelineDataToRetrieve dataToRetrieve = getDataToRetrieve();
    // 检查是否不是获取所有字段，或者指定了特定配置/指标需要获取
    boolean flag =
        !dataToRetrieve.getFieldsToRetrieve().contains(Field.ALL)
            || (dataToRetrieve.getConfsToRetrieve() != null && !dataToRetrieve
                .getConfsToRetrieve().getFilterList().isEmpty())
            || (dataToRetrieve.getMetricsToRetrieve() != null && !dataToRetrieve
                .getMetricsToRetrieve().getFilterList().isEmpty());
    // 如果上述条件不满足，多实体读取场景下检查是否有关联过滤或事件过滤
    if (!flag && !isSingleEntityRead()) {
      TimelineEntityFilters filters = getFilters();
      flag =
          (filters.getEventFilters() != null && !filters.getEventFilters()
              .getFilterList().isEmpty())
              || (filters.getIsRelatedTo() != null && !filters.getIsRelatedTo()
                  .getFilterList().isEmpty())
              || (filters.getRelatesTo() != null && !filters.getRelatesTo()
                  .getFilterList().isEmpty());
    }
    return flag;
  }

  /**
   * 将实体表固定列的限定符过滤添加到过滤列表。
   * @param list 目标过滤列表
   */
  protected void updateFixedColumns(FilterList list) {
    for (EntityColumn column : EntityColumn.values()) {
      list.addFilter(new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(
          column.getColumnQualifierBytes())));
    }
  }

  /**
   * 创建info列族的列过滤列表，只返回符合条件的限定符。
   * @return info列族过滤列表
   * @throws IOException 创建过滤列表时发生IO异常
   */
  private FilterList createFilterListForColsOfInfoFamily() throws IOException {
    FilterList infoFamilyColsFilter = new FilterList(Operator.MUST_PASS_ONE);
    // 添加固定列的过滤
    updateFixedColumns(infoFamilyColsFilter);
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // 如果需要获取INFO字段，添加INFO前缀列的过滤
    if (hasField(fieldsToRetrieve, Field.INFO)) {
      infoFamilyColsFilter
          .addFilter(TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, EntityColumnPrefix.INFO));
    }
    TimelineFilterList relatesTo = getFilters().getRelatesTo();
    if (hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      // 如果需要获取RELATES_TO字段，添加RELATES_TO前缀列的过滤
      infoFamilyColsFilter.addFilter(TimelineFilterUtils
          .createHBaseQualifierFilter(CompareOp.EQUAL,
              EntityColumnPrefix.RELATES_TO));
    } else if (relatesTo != null && !relatesTo.getFilterList().isEmpty()) {
      // 即使不需要整个RELATES_TO字段，仍然需要获取过滤条件指定的列用于后端过滤
      Set<String> relatesToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(relatesTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          EntityColumnPrefix.RELATES_TO, relatesToCols));
    }
    TimelineFilterList isRelatedTo = getFilters().getIsRelatedTo();
    if (hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      // 如果需要获取IS_RELATED_TO字段，添加IS_RELATED_TO前缀列的过滤
      infoFamilyColsFilter.addFilter(TimelineFilterUtils
          .createHBaseQualifierFilter(CompareOp.EQUAL,
              EntityColumnPrefix.IS_RELATED_TO));
    } else if (isRelatedTo != null && !isRelatedTo.getFilterList().isEmpty()) {
      // 即使不需要整个IS_RELATED_TO字段，仍然需要获取过滤条件指定的列用于后端过滤
      Set<String> isRelatedToCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(isRelatedTo);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          EntityColumnPrefix.IS_RELATED_TO, isRelatedToCols));
    }
    TimelineFilterList eventFilters = getFilters().getEventFilters();
    if (hasField(fieldsToRetrieve, Field.EVENTS)) {
      // 如果需要获取EVENTS字段，添加EVENT前缀列的过滤
      infoFamilyColsFilter
          .addFilter(TimelineFilterUtils.createHBaseQualifierFilter(
              CompareOp.EQUAL, EntityColumnPrefix.EVENT));
    } else if (eventFilters != null &&
        !eventFilters.getFilterList().isEmpty()) {
      // 即使不需要整个EVENTS字段，仍然需要获取过滤条件指定的列用于后端过滤
      Set<String> eventCols =
          TimelineFilterUtils.fetchColumnsFromFilterList(eventFilters);
      infoFamilyColsFilter.addFilter(createFiltersFromColumnQualifiers(
          EntityColumnPrefix.EVENT, eventCols));
    }
    return infoFamilyColsFilter;
  }

  /**
   * 根据需要获取的字段，排除info列族中不需要的列前缀。
   * @param infoColFamilyList info列族过滤列表
   */
  private void excludeFieldsFromInfoColFamily(FilterList infoColFamilyList) {
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    // 排除不需要的事件列
    if (!hasField(fieldsToRetrieve, Field.EVENTS)) {
      infoColFamilyList.addFilter(TimelineFilterUtils
          .createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              EntityColumnPrefix.EVENT));
    }
    // 排除不需要的info列
    if (!hasField(fieldsToRetrieve, Field.INFO)) {
      infoColFamilyList.addFilter(TimelineFilterUtils
          .createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              EntityColumnPrefix.INFO));
    }
    // 排除不需要的is_related_to列
    if (!hasField(fieldsToRetrieve, Field.IS_RELATED_TO)) {
      infoColFamilyList.addFilter(TimelineFilterUtils
          .createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              EntityColumnPrefix.IS_RELATED_TO));
    }
    // 排除不需要的relates_to列
    if (!hasField(fieldsToRetrieve, Field.RELATES_TO)) {
      infoColFamilyList.addFilter(TimelineFilterUtils
          .createHBaseQualifierFilter(CompareOp.NOT_EQUAL,
              EntityColumnPrefix.RELATES_TO));
    }
  }

  /**
   * 根据需要获取的配置和指标更新过滤列表。
   * @param listBasedOnFields 基于字段的过滤列表
   * @param cfsInFields 需要获取的列族集合
   * @throws IOException 更新过滤列表时发生IO异常
   */
  private void updateFilterForConfsAndMetricsToRetrieve(
      FilterList listBasedOnFields, Set<String> cfsInFields)
      throws IOException {
    TimelineDataToRetrieve dataToRetrieve = getDataToRetrieve();
    // 如果指定了配置需要获取，已经在augmentParams中将CONFIGS加入字段列表
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.CONFIGS)) {
      // 创建配置获取过滤列表
      listBasedOnFields.addFilter(TimelineFilterUtils
          .createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getConfsToRetrieve(), EntityColumnFamily.CONFIGS,
              EntityColumnPrefix.CONFIG));
      cfsInFields.add(Bytes.toString(EntityColumnFamily.CONFIGS.getBytes()));
    }

    // 如果指定了指标需要获取，已经在augmentParams中将METRICS加入字段列表
    if (dataToRetrieve.getFieldsToRetrieve().contains(Field.METRICS)) {
      // 创建指标获取过滤列表
      listBasedOnFields.addFilter(TimelineFilterUtils
          .createFilterForConfsOrMetricsToRetrieve(
              dataToRetrieve.getMetricsToRetrieve(),
              EntityColumnFamily.METRICS, EntityColumnPrefix.METRIC));
      cfsInFields.add(Bytes.toString(EntityColumnFamily.METRICS.getBytes()));
    }
  }

  @Override
  protected FilterList constructFilterListBasedOnFields(Set<String> cfsInFields)
      throws