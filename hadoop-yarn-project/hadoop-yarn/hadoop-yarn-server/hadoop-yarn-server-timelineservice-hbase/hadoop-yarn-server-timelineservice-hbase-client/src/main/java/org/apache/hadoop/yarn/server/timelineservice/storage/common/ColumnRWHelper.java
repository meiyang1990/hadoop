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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 文件说明：HBase列读写工具类，提供时间线服务存储层通用的列读写能力
 * 仅供具体列实现类内部使用，不暴露给客户端直接写入
 * A set of utility functions that read or read to a column.
 * This class is meant to be used only by explicit Columns,
 * and not directly to write by clients.
 */
public final class ColumnRWHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(ColumnHelper.class);

  private ColumnRWHelper() {
  }

  /**
   * 计算写入HBase时使用的单元格时间戳
   * 如果需要补充时间戳（流运行记录表场景），会左移原始时间戳并填入应用ID后缀，避免单元格冲突
   * Figures out the cell timestamp used in the Put For storing.
   * Will supplement the timestamp if required. Typically done for flow run
   * table.If we supplement the timestamp, we left shift the timestamp and
   * supplement it with the AppId id so that there are no collisions in the flow
   * run table's cells.
   */
  private static long getPutTimestamp(
      Long timestamp, boolean supplementTs, Attribute[] attributes) {
    if (timestamp == null) {
      timestamp = System.currentTimeMillis();
    }
    if (!supplementTs) {
      return timestamp;
    } else {
      String appId = getAppIdFromAttributes(attributes);
      long supplementedTS = TimestampGenerator.getSupplementedTimestamp(
          timestamp, appId);
      return supplementedTS;
    }
  }

  /**
   * 从属性数组中提取应用ID
   * @param attributes 属性数组
   * @return 提取到的应用ID，未找到返回null
   */
  private static String getAppIdFromAttributes(Attribute[] attributes) {
    if (attributes == null) {
      return null;
    }
    String appId = null;
    for (Attribute attribute : attributes) {
      if (AggregationCompactionDimension.APPLICATION_ID.toString().equals(
          attribute.getName())) {
        appId = Bytes.toString(attribute.getValue());
      }
    }
    return appId;
  }

  /**
   * 写入单个列数据到HBase，将变更缓冲后批量发送
   *
   * @param rowKey HBase行键，null则不写入
   * @param tableMutator HBase表变更缓冲器
   * @param column 要写入的列对象
   * @param timestamp 版本时间戳，null则自动生成补充后的时间戳
   * @param inputValue 要写入的值，null则不写入
   * @param attributes HBase Put操作属性
   * @throws IOException 写入HBase时抛出异常
   */
  public static void store(byte[] rowKey, TypedBufferedMutator<?> tableMutator,
                           Column<?> column, Long timestamp,
                           Object inputValue, Attribute... attributes)
      throws IOException {
    store(rowKey, tableMutator, column.getColumnFamilyBytes(),
        column.getColumnQualifierBytes(), timestamp,
        column.supplementCellTimestamp(), inputValue,
        column.getValueConverter(),
        column.getCombinedAttrsWithAggr(attributes));
  }

  /**
   * 写入单个列数据到HBase，将变更缓冲后批量发送（底层实现）
   *
   * @param rowKey HBase行键，null则不写入
   * @param tableMutator HBase表变更缓冲器
   * @param columnFamilyBytes 列族字节数组
   * @param columnQualifier 列限定符，null则不写入
   * @param timestamp 版本时间戳，null则自动生成补充后的时间戳
   * @param supplementTs 是否需要补充时间戳避免冲突
   * @param inputValue 要写入的值，null则不写入
   * @param converter 值转换器，用于编码写入值
   * @param attributes HBase Put操作属性
   * @throws IOException 写入HBase时抛出异常
   */
  public static void store(byte[] rowKey, TypedBufferedMutator<?> tableMutator,
      byte[] columnFamilyBytes, byte[] columnQualifier, Long timestamp,
      boolean supplementTs, Object inputValue, ValueConverter converter,
      Attribute... attributes) throws IOException {
    if ((rowKey == null) || (columnQualifier == null) || (inputValue == null)) {
      return;
    }
    Put p = new Put(rowKey);
    timestamp = getPutTimestamp(timestamp, supplementTs, attributes);
    p.addColumn(columnFamilyBytes, columnQualifier, timestamp,
        converter.encodeValue(inputValue));
    if ((attributes != null) && (attributes.length > 0)) {
      for (Attribute attribute : attributes) {
        p.setAttribute(attribute.getName(), attribute.getValue());
      }
    }
    tableMutator.mutate(p);
  }

  /**
   * 从HBase Result中读取指定列的最新版本值
   *
   * @param result HBase查询结果，不能为null
   * @param columnFamilyBytes 列族字节数组
   * @param columnQualifierBytes 要读取的列限定符
   * @param converter 值转换器，用于解码读取值
   * @return 解码后的对象，不存在则返回null
   * @throws IOException 解码值时抛出异常
   */
  public static Object readResult(Result result, byte[] columnFamilyBytes,
      byte[] columnQualifierBytes, ValueConverter converter)
      throws IOException {
    if (result == null || columnQualifierBytes == null) {
      return null;
    }

    // Would have preferred to be able to use getValueAsByteBuffer and get a
    // ByteBuffer to avoid copy, but GenericObjectMapper doesn't seem to like
    // that.
    byte[] value = result.getValue(columnFamilyBytes, columnQualifierBytes);
    return converter.decodeValue(value);
  }

  /**
   * 从HBase Result中读取指定列的最新版本值
   *
   * @param result HBase查询结果，不能为null
   * @param column 要读取的列对象
   * @return 解码后的对象，不存在则返回null
   * @throws IOException 解码值时抛出异常
   */
  public static Object readResult(Result result, Column<?> column)
      throws IOException {
    return readResult(result, column.getColumnFamilyBytes(),
        column.getColumnQualifierBytes(), column.getValueConverter());
  }

  /**
   * 从HBase Result中读取带前缀列的最新版本值
   *
   * @param result HBase查询结果，不能为null
   * @param columnPrefix 列前缀对象
   * @param qualifier 列限定后缀
   * @return 解码后的对象，不存在则返回null
   * @throws IOException 解码值时抛出异常
   */
  public static Object readResult(Result result, ColumnPrefix<?> columnPrefix,
                                  String qualifier) throws IOException {
    byte[] columnQualifier = ColumnHelper.getColumnQualifier(
        columnPrefix.getColumnPrefixInBytes(), qualifier);

    return readResult(
        result, columnPrefix.getColumnFamilyBytes(),
        columnQualifier, columnPrefix.getValueConverter());
  }

  /**
   * 从HBase Result中批量读取带前缀的所有列的最新版本值
   *
   * @param <K> 键类型
   * @param result HBase查询结果
   * @param columnPrefix 列前缀对象
   * @param keyConverter 列键转换器，将字节转换为指定类型
   * @return 列键到值的映射表
   * @throws IOException 读取或解码时抛出异常
   */
  public static <K> Map<K, Object> readResults(Result result,
      ColumnPrefix<?> columnPrefix, KeyConverter<K> keyConverter)
      throws IOException {
    return readResults(result,
        columnPrefix.getColumnFamilyBytes(),
        columnPrefix.getColumnPrefixInBytes(),
        keyConverter, columnPrefix.getValueConverter());
  }

  /**
   * 从HBase Result中批量读取带前缀列的所有版本（按时间戳组织）
   *
   * @param <K> 键类型
   * @param <V> 值类型
   * @param result HBase查询结果
   * @param columnPrefix 列前缀对象
   * @param keyConverter 列键转换器，将字节转换为指定类型
   * @return 层级映射表 {列键 -> {时间戳 -> 值}}
   * @throws IOException 读取或解码时抛出异常
   */
  public static <K, V> NavigableMap<K, NavigableMap<Long, V>>
      readResultsWithTimestamps(Result result, ColumnPrefix<?> columnPrefix,
      KeyConverter<K> keyConverter) throws IOException {
    return readResultsWithTimestamps(result,
        columnPrefix.getColumnFamilyBytes(),
        columnPrefix.getColumnPrefixInBytes(),
        keyConverter, columnPrefix.getValueConverter(),
        columnPrefix.supplementCellTimeStamp());
  }

  /**
   * 从HBase Result中批量读取带前缀列的所有版本（按时间戳组织，底层实现）
   *
   * @param result HBase查询结果
   * @param columnFamilyBytes 列族字节数组
   * @param columnPrefixBytes 列前缀字节数组，null则返回所有列
   * @param <K> 键类型
   * @param <V> 值类型
   * @param keyConverter 列键转换器，将字节转换为指定类型
   * @param valueConverter 值转换器，用于解码读取值
   * @param supplementTs 是否需要截断补充的时间戳
   * @return 层级映射表 {列键 -> {时间戳 -> 值}}
   * @throws IOException 读取或解码时抛出异常
   */
  @SuppressWarnings("unchecked")
  public static <K, V> NavigableMap<K, NavigableMap<Long, V>>
      readResultsWithTimestamps(Result result, byte[] columnFamilyBytes,
          byte[] columnPrefixBytes, KeyConverter<K> keyConverter,
          ValueConverter valueConverter, boolean supplementTs)
      throws IOException {

    NavigableMap<K, NavigableMap<Long, V>> results = new TreeMap<>();

    if (result != null) {
      NavigableMap<
          byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap =
          result.getMap();

      NavigableMap<byte[], NavigableMap<Long, byte[]>> columnCellMap =
          resultMap.get(columnFamilyBytes);
      // could be that there is no such column family.
      if (columnCellMap != null) {
        // 遍历当前列族下所有列
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : columnCellMap
            .entrySet()) {
          K converterColumnKey = null;
          if (columnPrefixBytes == null) {
            // 无前缀，返回所有列
            LOG.debug("null prefix was specified; returning all columns");
            try {
              converterColumnKey = keyConverter.decode(entry.getKey());
            } catch (IllegalArgumentException iae) {
              LOG.error("Illegal column found, skipping this column.", iae);
              continue;
            }
          } else {
            // 有前缀，列格式为 前缀!列名后缀，拆分后匹配前缀
            byte[][] columnNameParts =
                Separator.QUALIFIERS.split(entry.getKey(), 2);
            byte[] actualColumnPrefixBytes = columnNameParts[0];
            if (Bytes.equals(columnPrefixBytes, actualColumnPrefixBytes)
                && columnNameParts.length == 2) {
              try {
                // 匹配前缀成功，解码后缀为列键
                converterColumnKey = keyConverter.decode(columnNameParts[1]);
              } catch (IllegalArgumentException iae) {
                LOG.error("Illegal column found, skipping this column.", iae);
                continue;
              }
            }
          }

          // 列匹配成功，处理所有时间戳版本
          if (converterColumnKey != null) {
            NavigableMap<Long, V> cellResults =
                new TreeMap<Long, V>();
            NavigableMap<Long, byte[]> cells = entry.getValue();
            if (cells != null) {
              for (Map.Entry<Long, byte[]> cell : cells.entrySet()) {
                V value =
                    (V) valueConverter.decodeValue(cell.getValue());
                // 如果是补充过的时间戳，截断还原原始时间戳
                long ts = supplementTs ? TimestampGenerator.
                    getTruncatedTimestamp(cell.getKey()) : cell.getKey();
                cellResults.put(ts, value);
              }
            }
            results.put(converterColumnKey, cellResults);
          }
        } // for entry : columnCellMap
      } // if columnCellMap != null
    } // if result != null
    return results;
  }

  /**
   * 从HBase Result中批量读取带前缀的所有列的最新版本值（底层实现）
   *
   * @param <K> 键类型
   * @param result HBase查询结果
   * @param columnFamilyBytes 列族字节数组
   * @param columnPrefixBytes 列前缀字节数组，null则返回所有列
   * @param keyConverter 列键转换器，将字节转换为指定类型
   * @param valueConverter 值转换器，用于解码读取值
   * @return 列键到值的映射表
   * @throws IOException 读取或解码时抛出异常
   */
  public static <K> Map<K, Object> readResults(Result result,
      byte[] columnFamilyBytes, byte[] columnPrefixBytes,
      KeyConverter<K> keyConverter, ValueConverter valueConverter)
      throws IOException {
    Map<K, Object> results = new HashMap<K, Object>();

    if (result != null) {
      Map<byte[], byte[]> columns = result.getFamilyMap(columnFamilyBytes);
      // 遍历当前列族下所有列
      for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
        byte[] columnKey = entry.getKey();
        if (columnKey != null && columnKey.length > 0) {

          K converterColumnKey = null;
          if (columnPrefixBytes == null) {
            // 无前缀，返回所有列
            try {
              converterColumnKey = keyConverter.decode(columnKey);
            } catch (IllegalArgumentException iae) {
              LOG.error("Illegal column found, skipping this column.", iae);
              continue;
            }
          } else {
            // 有前缀，列格式为 前缀!列名后缀，拆分后匹配前缀
            byte[][] columnNameParts = Separator.QUALIFIERS.split(columnKey, 2);
            if (columnNameParts.length > 0) {
              byte[] actualColumnPrefixBytes = columnNameParts[0];
              // If this is the prefix that we want
              if (Bytes.equals(columnPrefixBytes, actualColumnPrefixBytes)
                  && columnNameParts.length == 2) {
                try {
                  // 匹配前缀成功，解码后缀为列键
                  converterColumnKey = keyConverter.decode(columnNameParts[1]);
                } catch (IllegalArgumentException iae) {
                  LOG.error("Illegal column found, skipping this column.", iae);
                  continue;
                }
              }
            }
          } // if-else

          // 列匹配成功，解码值存入结果
          if (converterColumnKey != null) {
            Object value = valueConverter.decodeValue(entry.getValue());
            // we return the columnQualifier in parts since we don't know
            // which part is of which data type.
            results.put(converterColumnKey, value);
          }
        }
      } // for entry
    }
    return results;
  }

  /**
   * 写入带前缀的字节限定符列数据到HBase
   *
   * @param rowKey HBase行键，null则不写入
   * @param tableMutator HBase表变更缓冲器
   * @param columnPrefix 列前缀对象
   * @param qualifier 字节类型列限定后缀
   * @param timestamp 版本时间戳，null则自动生成
   * @param inputValue 要写入的值，null则不写入
   * @param attributes HBase Put操作属性
   * @throws IOException 写入失败或限定符为null时抛出异常
   */
  public static void store(byte[] rowKey, TypedBufferedMutator<?> tableMutator,
             ColumnPrefix<?> columnPrefix, byte[] qualifier, Long timestamp