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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnName;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnNameConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.StringKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：从HBase存储中读取时间线实体的抽象基类，为不同类型实体查询提供统一读取框架。
 * 定义了查询过滤、结果解析等公共流程，子类负责实现不同场景的具体查询逻辑。
 */
/**
 * The base class for reading and deserializing timeline entities from the
 * HBase storage. Different types can be defined for different types of the
 * entities that are being requested.
 */
public abstract class TimelineEntityReader extends
    AbstractTimelineStorageReader {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineEntityReader.class);

  /** 是否为单实体读取模式 */
  private final boolean singleEntityRead;
  /** 需要检索的数据配置 */
  private TimelineDataToRetrieve dataToRetrieve;
  // used only for multiple entity read mode
  /** 多实体读取模式下的过滤条件 */
  private TimelineEntityFilters filters;

  /**
   * Main table the entity reader uses.
   */
  /** 读取使用的主表 */
  private BaseTableRW<?> table;

  /**
   * Used to convert strings key components to and from storage format.
   */
  /** 字符串键与存储格式的转换器 */
  private final KeyConverter<String> stringKeyConverter =
      new StringKeyConverter();

  /**
   * Instantiates a reader for multiple-entity reads.
   *
   * @param ctxt Reader context which defines the scope in which query has to be
   *     made.
   * @param entityFilters Filters which limit the entities returned.
   * @param toRetrieve Data to retrieve for each entity.
   */
  protected TimelineEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt);
    this.singleEntityRead = false;
    this.dataToRetrieve = toRetrieve;
    this.filters = entityFilters;

    this.setTable(getTable());
  }

  /**
   * Instantiates a reader for single-entity reads.
   *
   * @param ctxt Reader context which defines the scope in which query has to be
   *     made.
   * @param toRetrieve Data to retrieve for each entity.
   */
  protected TimelineEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt);
    this.singleEntityRead = true;
    this.dataToRetrieve = toRetrieve;

    this.setTable(getTable());
  }

  /**
   * 基于需要检索的字段构造HBase过滤器列表，仅用于多实体读取。
   * 用于过滤HBase返回结果，减少数据传输量。
   *
   * @param cfsInFields 输出参数，收集字段涉及的列族
   * @return a {@link FilterList} object.
   * @throws IOException if any problem occurs while creating filter list.
   */
  protected abstract FilterList constructFilterListBasedOnFields(
      Set<String> cfsInFields) throws IOException;

  /**
   * 基于过滤条件构造HBase过滤器列表，用于单实体读取。
   * 修剪从HBase获取的结果，仅返回符合条件的数据。
   *
   * @return a {@link FilterList} object.
   * @throws IOException if any problem occurs while creating filter list.
   */
  protected abstract FilterList constructFilterListBasedOnFilters()
      throws IOException;

  /**
   * Combines filter lists created based on fields and based on filters.
   *
   * @return a {@link FilterList} object if it can be constructed. Returns null,
   * if filter list cannot be created either on the basis of filters or on the
   * basis of fields.
   * @throws IOException if any problem occurs while creating filter list.
   */
  private FilterList createFilterList() throws IOException {
    // 构造基于过滤条件的过滤器列表
    FilterList listBasedOnFilters = constructFilterListBasedOnFilters();
    boolean hasListBasedOnFilters = listBasedOnFilters != null &&
        !listBasedOnFilters.getFilters().isEmpty();
    // 构造基于检索字段的过滤器列表
    Set<String> cfsInListBasedOnFields = new HashSet<>(0);
    FilterList listBasedOnFields =
        constructFilterListBasedOnFields(cfsInListBasedOnFields);
    boolean hasListBasedOnFields = listBasedOnFields != null &&
        !listBasedOnFields.getFilters().isEmpty();
    // If filter lists based on both filters and fields can be created,
    // combine them in a new filter list and return it.
    // If either one of them has been created, return that filter list.
    // Return null, if none of the filter lists can be created. This indicates
    // that no filter list needs to be added to HBase Scan as filters are not
    // specified for the query or only the default view of entity needs to be
    // returned.
    // 两种过滤器都存在时，合并后返回
    if (hasListBasedOnFilters && hasListBasedOnFields) {
      FilterList list = new FilterList();
      list.addFilter(listBasedOnFilters);

      // 提取过滤条件涉及的列族
      Set<String> cfsInListBasedOnFilters = new HashSet<>(0);
      extractColumnFamiliesFromFiltersBasedOnFilters(
          listBasedOnFilters, cfsInListBasedOnFilters);

      // 移除字段过滤器已经包含的列族，避免重复返回整个列族
      cfsInListBasedOnFilters.removeAll(cfsInListBasedOnFields);

      // 为未包含的列族添加列族过滤器
      if (!cfsInListBasedOnFilters.isEmpty()) {
        for (String cf: cfsInListBasedOnFilters) {
          listBasedOnFields.addFilter(new FamilyFilter(CompareOp.EQUAL,
              new BinaryComparator(Bytes.toBytes(cf))));
        }
      }
      list.addFilter(listBasedOnFields);
      return list;
    } else if (hasListBasedOnFilters) {
      // 仅存在过滤条件过滤器，直接返回
      return listBasedOnFilters;
    } else if (hasListBasedOnFields) {
      // 仅存在字段过滤器，直接返回
      return listBasedOnFields;
    }
    // 都不存在，返回null
    return null;
  }

  /**
   * 递归从HBase过滤器中提取涉及的所有列族，用于后续添加列族过滤。
   * @param hbaseFilterBasedOnTLSFilter 当前处理的HBase过滤器
   * @param columnFamilies 输出参数，收集提取到的列族
   */
  private static void extractColumnFamiliesFromFiltersBasedOnFilters(
      Filter hbaseFilterBasedOnTLSFilter, Set<String> columnFamilies) {
    // 单值过滤器直接提取列族
    if (hbaseFilterBasedOnTLSFilter instanceof SingleColumnValueFilter) {
      byte[] cf =  ((SingleColumnValueFilter)
          hbaseFilterBasedOnTLSFilter).getFamily();
      columnFamilies.add(Bytes.toString(cf));
    } else if (hbaseFilterBasedOnTLSFilter instanceof FilterList) {
      // 过滤器列表递归提取每个子过滤器的列族
      FilterList filterListBase = (FilterList) hbaseFilterBasedOnTLSFilter;
      for (Filter fs: filterListBase.getFilters()) {
        extractColumnFamiliesFromFiltersBasedOnFilters(fs, columnFamilies);
      }
    }
  }


  protected TimelineDataToRetrieve getDataToRetrieve() {
    return dataToRetrieve;
  }

  protected TimelineEntityFilters getFilters() {
    return filters;
  }

  /**
   * Create a {@link TimelineEntityFilters} object with default values for
   * filters.
   */
  /** 如果过滤条件为空，创建默认过滤条件对象 */
  protected void createFiltersIfNull() {
    if (filters == null) {
      filters = new TimelineEntityFilters.Builder().build();
    }
  }

  /**
   * Reads and deserializes a single timeline entity from the HBase storage.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @return A <cite>TimelineEntity</cite> object.
   * @throws IOException if there is any exception encountered while reading
   *     entity.
   */
  public TimelineEntity readEntity(Configuration hbaseConf, Connection conn)
      throws IOException {
    // 验证查询参数
    validateParams();
    // 补充查询参数
    augmentParams(hbaseConf, conn);

    // 基于检索字段构造过滤器
    FilterList filterList = constructFilterListBasedOnFields(new HashSet<>(0));
    if (filterList != null) {
      LOG.debug("FilterList created for get is - {}", filterList);
    }
    // 从HBase获取查询结果
    Result result = getResult(hbaseConf, conn, filterList);
    if (result == null || result.isEmpty()) {
      // Could not find a matching row.
      LOG.info("Cannot find matching entity of type " +
          getContext().getEntityType());
      return null;
    }
    // 解析结果为时间线实体对象并返回
    return parseEntity(result);
  }

  /**
   * Reads and deserializes a set of timeline entities from the HBase storage.
   * It goes through all the results available, and returns the number of
   * entries as specified in the limit in the entity's natural sort order.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @return a set of <cite>TimelineEntity</cite> objects.
   * @throws IOException if any exception is encountered while reading entities.
   */
  public Set<TimelineEntity> readEntities(Configuration hbaseConf,
      Connection conn) throws IOException {
    // 验证查询参数
    validateParams();
    // 补充查询参数
    augmentParams(hbaseConf, conn);

    Set<TimelineEntity> entities = new LinkedHashSet<>();
    // 创建合并后的过滤器列表
    FilterList filterList = createFilterList();
    if (filterList != null) {
      LOG.debug("FilterList created for scan is - {}", filterList);
    }
    // 从HBase获取扫描结果
    ResultScanner results = getResults(hbaseConf, conn, filterList);
    try {
      // 遍历结果逐个解析
      for (Result result : results) {
        TimelineEntity entity = parseEntity(result);
        if (entity == null) {
          continue;
        }
        entities.add(entity);
        // 达到数量限制则停止遍历
        if (entities.size() == filters.getLimit()) {
          break;
        }
      }
      return entities;
    } finally {
      // 确保结果扫描器关闭
      results.close();
    }
  }

  /**
   * Returns the main table to be used by the entity reader.
   *
   * @return A reference to the table.
   */
  protected BaseTableRW<?> getTable() {
    return table;
  }

  /**
   * Fetches a {@link Result} instance for a single-entity read.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @param filterList filter list which will be applied to HBase Get.
   * @return the {@link Result} instance or null if no such record is found.
   * @throws IOException if any exception is encountered while getting result.
   */
  protected abstract Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException;

  /**
   * Fetches a {@link ResultScanner} for a multi-entity read.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @param filterList filter list which will be applied to HBase Scan.
   * @return the {@link ResultScanner} instance.
   * @throws IOException if any exception is encountered while getting results.
   */
  protected abstract ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException;

  /**
   * Parses the result retrieved from HBase backend and convert it into a
   * {@link TimelineEntity} object.
   *
   * @param result Single row result of a Get/Scan.
   * @return the <cite>TimelineEntity</cite> instance or null if the entity is
   *     filtered.
   * @throws IOException if any exception is encountered while parsing entity.
   */
  protected abstract TimelineEntity parseEntity(Result result)
      throws IOException;

  /**
   * Helper method for reading and deserializing {@link TimelineMetric} objects
   * using the specified column prefix. The timeline metrics then are added to
   * the given timeline entity.
   *
   * @param entity {@link TimelineEntity} object.
   * @param result {@link Result} object retrieved from backend.
   * @param columnPrefix Metric column prefix
   * @throws IOException if any exception is encountered while reading metrics.
   */
  protected void readMetrics(TimelineEntity entity, Result result,
      ColumnPrefix<?> columnPrefix) throws IOException {
    // 读取带时间戳的指标结果
    NavigableMap<String, NavigableMap<Long, Number>> metricsResult =
        ColumnRWHelper.readResultsWithTimestamps(
            result, columnPrefix, stringKeyConverter);
    // 遍历转换每个指标
    for (Map.Entry<String, NavigableMap<Long, Number>> metricResult:
        metricsResult.entrySet()) {
      TimelineMetric metric = new TimelineMetric();
      metric.setId(metricResult.getKey());
      // 根据值个数判断指标类型：多个值为时间序列，单个值为单值指标
      TimelineMetric.Type metricType = metricResult.getValue().size() > 1 ?
          TimelineMetric.Type.TIME_SERIES : TimelineMetric.Type.SINGLE_VALUE;
      metric.setType(metricType);
      metric.addValues(metricResult.getValue());
      entity.addMetric(metric);
    }
  }

  /**
   * Checks whether the reader has been created to fetch single entity or
   * multiple entities.
   *
   * @return true, if query is for single entity, false otherwise.
   */
  public boolean isSingleEntityRead() {
    return singleEntityRead;
  }

  protected void setTable(BaseTableRW<?> baseTable) {
    this.table = baseTable;