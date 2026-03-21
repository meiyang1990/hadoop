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

package org.apache.hadoop.yarn.server.timelineservice.reader.filter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：时间线服务HBase存储过滤工具类，提供时间线过滤器到HBase过滤器的转换功能
 * 为时间线查询处理提供HBase层的过滤逻辑封装
 * Set of utility methods used by timeline filter classes.
 */
public final class TimelineFilterUtils {

  /**
   * 日志记录器
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineFilterUtils.class);

  /**
   * 工具类禁止实例化
   */
  private TimelineFilterUtils() {
  }

  /**
   * 将时间线过滤器列表操作符转换为等价的HBase过滤器列表操作符
   *
   * @param op timeline filter list operator.
   * @return HBase filter list's Operator.
   */
  private static Operator getHBaseOperator(TimelineFilterList.Operator op) {
    switch (op) {
    case AND:
      return Operator.MUST_PASS_ALL;
    case OR:
      return Operator.MUST_PASS_ONE;
    default:
      throw new IllegalArgumentException("Invalid operator");
    }
  }

  /**
   * 将时间线比较操作符转换为等价的HBase比较操作符
   *
   * @param op timeline compare op.
   * @return HBase compare filter's CompareOp.
   */
  private static CompareOp getHBaseCompareOp(
      TimelineCompareOp op) {
    switch (op) {
    case LESS_THAN:
      return CompareOp.LESS;
    case LESS_OR_EQUAL:
      return CompareOp.LESS_OR_EQUAL;
    case EQUAL:
      return CompareOp.EQUAL;
    case NOT_EQUAL:
      return CompareOp.NOT_EQUAL;
    case GREATER_OR_EQUAL:
      return CompareOp.GREATER_OR_EQUAL;
    case GREATER_THAN:
      return CompareOp.GREATER;
    default:
      throw new IllegalArgumentException("Invalid compare operator");
    }
  }

  /**
   * 将时间线前缀过滤器转换为等价的HBase列限定符前缀过滤器
   * @param colPrefix 列前缀
   * @param filter 时间线前缀过滤器
   * @return HBase QualifierFilter 对象
   */
  private static <T extends BaseTable<T>> Filter createHBaseColQualPrefixFilter(
      ColumnPrefix<T> colPrefix, TimelinePrefixFilter filter) {
    return new QualifierFilter(getHBaseCompareOp(filter.getCompareOp()),
        new BinaryPrefixComparator(
            colPrefix.getColumnPrefixBytes(filter.getPrefix())));
  }

  /**
   * 根据列前缀创建HBase列限定符前缀过滤器
   *
   * @param <T> 列前缀类型
   * @param compareOp 比较操作符
   * @param columnPrefix 列前缀
   * @return HBase列限定符过滤器
   */
  public static <T extends BaseTable<T>> Filter createHBaseQualifierFilter(
      CompareOp compareOp, ColumnPrefix<T> columnPrefix) {
    return new QualifierFilter(compareOp,
        new BinaryPrefixComparator(
            columnPrefix.getColumnPrefixBytes("")));
  }

  /**
   * 为需要检索的配置或指标创建HBase过滤器列表，包含列族过滤器和对应指标/配置过滤规则
   *
   * @param <T> 列前缀类型
   * @param confsOrMetricToRetrieve 需要检索的配置/指标过滤列表
   * @param columnFamily 配置或指标对应的列族
   * @param columnPrefix 配置或指标对应的列前缀
   * @return 组装完成的过滤器列表
   * @throws IOException 创建过滤器过程中出现错误时抛出
   */
  public static <T extends BaseTable<T>> Filter
      createFilterForConfsOrMetricsToRetrieve(
      TimelineFilterList confsOrMetricToRetrieve, ColumnFamily<T> columnFamily,
      ColumnPrefix<T> columnPrefix) throws IOException {
    // 创建列族相等过滤器
    Filter familyFilter = new FamilyFilter(CompareOp.EQUAL,
        new BinaryComparator(columnFamily.getBytes()));
    if (confsOrMetricToRetrieve != null &&
        !confsOrMetricToRetrieve.getFilterList().isEmpty()) {
      // 如果指定了需要检索的配置/指标，组合列族过滤器和对应条件过滤器
      FilterList filter = new FilterList(familyFilter);
      filter.addFilter(
          createHBaseFilterList(columnPrefix, confsOrMetricToRetrieve));
      return filter;
    } else {
      // 仅需要列族过滤，返回列族过滤器
      return familyFilter;
    }
  }

  /**
   * 根据值范围[startValue, endValue]创建两个单值过滤器，封装在过滤器列表中返回
   * 用于范围查询场景
   *
   * @param <T> 列前缀类型
   * @param column 需要过滤的列
   * @param startValue 范围起始值
   * @param endValue 范围结束值
   * @return 包含两个范围过滤器的过滤器列表
   * @throws IOException 编码值过程中出现错误时抛出
   */
  public static <T extends BaseTable<T>> FilterList
      createSingleColValueFiltersByRange(Column<T> column,
          Object startValue, Object endValue) throws IOException {
    FilterList list = new FilterList();
    // 添加 >= startValue 过滤器
    Filter singleColValFilterStart = createHBaseSingleColValueFilter(
        column.getColumnFamilyBytes(), column.getColumnQualifierBytes(),
        column.getValueConverter().encodeValue(startValue),
        CompareOp.GREATER_OR_EQUAL, true);
    list.addFilter(singleColValFilterStart);

    // 添加 <= endValue 过滤器
    Filter singleColValFilterEnd = createHBaseSingleColValueFilter(
        column.getColumnFamilyBytes(), column.getColumnQualifierBytes(),
        column.getValueConverter().encodeValue(endValue),
        CompareOp.LESS_OR_EQUAL, true);
    list.addFilter(singleColValFilterEnd);
    return list;
  }

  /**
   * 根据指定列和过滤值、比较操作符创建HBase单值过滤器
   * @param <T> 列前缀类型
   * @param column 需要过滤值的列
   * @param value 过滤值
   * @param op 比较操作符
   * @return HBase SingleColumnValueFilter 对象
   * @throws IOException 编码值过程中出现异常时抛出
   */
  public static <T extends BaseTable<T>> Filter
      createHBaseSingleColValueFilter(Column<T> column,
          Object value, CompareOp op) throws IOException {
    Filter singleColValFilter = createHBaseSingleColValueFilter(
        column.getColumnFamilyBytes(), column.getColumnQualifierBytes(),
        column.getValueConverter().encodeValue(value), op, true);
    return singleColValFilter;
  }

  /**
   * 创建HBase SingleColumnValueFilter底层实现
   *
   * @param columnFamily 列族字节数组
   * @param columnQualifier 列限定符字节数组
   * @param value 过滤值字节数组
   * @param compareOp 比较操作符
   * @param filterIfMissing 指定列不存在时是否过滤整行，对应HBase的keyMustExist字段
   * @return HBase SingleColumnValueFilter 对象
   * @throws IOException
   */
  private static SingleColumnValueFilter createHBaseSingleColValueFilter(
      byte[] columnFamily, byte[] columnQualifier, byte[] value,
      CompareOp compareOp, boolean filterIfMissing) throws IOException {
    SingleColumnValueFilter singleColValFilter =
        new SingleColumnValueFilter(columnFamily, columnQualifier, compareOp,
        new BinaryComparator(value));
    // 仅匹配最新版本的值
    singleColValFilter.setLatestVersionOnly(true);
    // 设置列不存在时是否过滤
    singleColValFilter.setFilterIfMissing(filterIfMissing);
    return singleColValFilter;
  }

  /**
   * 从过滤列表中提取所有需要查询的列名，用于仅加载必要列减少HBase数据读取量
   * 供后续在阅读器层完成事件和关系过滤
   *
   * @param filterList 时间线过滤列表
   * @return 需要查询的列名集合
   */
  public static Set<String> fetchColumnsFromFilterList(
      TimelineFilterList filterList) {
    Set<String> strSet = new HashSet<String>();
    // 遍历所有过滤器提取列名
    for (TimelineFilter filter : filterList.getFilterList()) {
      switch(filter.getFilterType()) {
      case LIST:
        // 递归处理嵌套过滤列表
        strSet.addAll(fetchColumnsFromFilterList((TimelineFilterList)filter));
        break;
      case KEY_VALUES:
        // 多值过滤，提取键
        strSet.add(((TimelineKeyValuesFilter)filter).getKey());
        break;
      case EXISTS:
        // 存在性过滤，提取值
        strSet.add(((TimelineExistsFilter)filter).getValue());
        break;
      default:
        LOG.info("Unexpected filter type " + filter.getFilterType());
        break;
      }
    }
    return strSet;
  }

  /**
   * 将时间线过滤列表转换为等价的HBase过滤列表，递归转换所有子过滤器
   *
   * @param <T> 列前缀类型
   * @param colPrefix 列前缀，用于转换过程生成HBase列信息
   * @param filterList 需要转换的时间线过滤列表
   * @return 转换后的HBase FilterList 对象
   * @throws IOException 创建过滤器过程中出错时抛出
   */
  public static <T extends BaseTable<T>> FilterList createHBaseFilterList(
      ColumnPrefix<T> colPrefix,
      TimelineFilterList filterList) throws IOException {
    // 根据时间线操作符创建对应HBase过滤列表
    FilterList list =
        new FilterList(getHBaseOperator(filterList.getOperator()));
    // 遍历转换每个过滤器
    for (TimelineFilter filter : filterList.getFilterList()) {
      switch(filter.getFilterType()) {
      case LIST:
        // 递归转换嵌套过滤列表
        list.addFilter(createHBaseFilterList(colPrefix,
            (TimelineFilterList)filter));
        break;
      case PREFIX:
        // 转换前缀过滤器
        list.addFilter(createHBaseColQualPrefixFilter(colPrefix,
            (TimelinePrefixFilter)filter));
        break;
      case COMPARE:
        // 转换比较过滤器
        TimelineCompareFilter compareFilter = (TimelineCompareFilter)filter;
        list.addFilter(
            createHBaseSingleColValueFilter(
                colPrefix.getColumnFamilyBytes(),
                colPrefix.getColumnPrefixBytes(compareFilter.getKey()),
                colPrefix.getValueConverter().
                    encodeValue(compareFilter.getValue()),
                getHBaseCompareOp(compareFilter.getCompareOp()),
                compareFilter.getKeyMustExist()));
        break;
      case KEY_VALUE:
        // 转换键值对过滤器
        TimelineKeyValueFilter kvFilter = (TimelineKeyValueFilter)filter;
        list.addFilter(
            createHBaseSingleColValueFilter(
                colPrefix.getColumnFamilyBytes(),
                colPrefix.getColumnPrefixBytes(kvFilter.getKey()),
                colPrefix.getValueConverter().encodeValue(kvFilter.getValue()),
                getHBaseCompareOp(kvFilter.getCompareOp()),
                kvFilter.getKeyMustExist()));
        break;
      default:
        LOG.info("Unexpected filter type " + filter.getFilterType());
        break;
      }
    }
    return list;
  }
}