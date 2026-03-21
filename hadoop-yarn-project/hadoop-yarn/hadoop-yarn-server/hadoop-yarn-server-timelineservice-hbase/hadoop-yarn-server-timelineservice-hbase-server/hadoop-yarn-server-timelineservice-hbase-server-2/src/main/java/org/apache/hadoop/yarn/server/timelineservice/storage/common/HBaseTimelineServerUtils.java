// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HBase时间线服务端工具类，为时间线数据在HBase存储提供通用辅助能力。
 */
public final class HBaseTimelineServerUtils {
  private HBaseTimelineServerUtils() {
  }

  /**
   * 从输入属性创建HBase Tag，支持聚合操作和聚合压缩维度两种类型。
   *
   * @param attribute 输入属性键值对
   * @return 转换后的HBase Tag，无法识别类型则返回null
   */
  public static Tag getTagFromAttribute(Map.Entry<String, byte[]> attribute) {
    // 先尝试识别是否为聚合操作
    AggregationOperation aggOp = AggregationOperation
        .getAggregationOperation(attribute.getKey());
    if (aggOp != null) {
      Tag t = createTag(aggOp.getTagType(), attribute.getValue());
      return t;
    }

    // 再尝试识别是否为聚合压缩维度
    AggregationCompactionDimension aggCompactDim =
        AggregationCompactionDimension.getAggregationCompactionDimension(
            attribute.getKey());
    if (aggCompactDim != null) {
      Tag t = createTag(aggCompactDim.getTagType(), attribute.getValue());
      return t;
    }
    return null;
  }

  /**
   * 基于原有Cell创建新Cell，仅替换值。
   *
   * @param origCell 原始Cell
   * @param newValue 新值
   * @return 新创建的Cell
   * @throws IOException 创建失败抛出
   */
  public static Cell createNewCell(Cell origCell, byte[] newValue)
      throws IOException {
    return CellUtil.createCell(CellUtil.cloneRow(origCell),
        CellUtil.cloneFamily(origCell), CellUtil.cloneQualifier(origCell),
        origCell.getTimestamp(), KeyValue.Type.Put.getCode(), newValue);
  }

  /**
   * 基于输入参数创建全新Cell。
   *
   * @param row 行键
   * @param family 列族
   * @param qualifier 列限定符
   * @param ts 时间戳
   * @param newValue 单元格值
   * @param tags 标签字节数组
   * @return 新创建的Cell
   * @throws IOException 创建失败抛出
   */
  public static Cell createNewCell(byte[] row, byte[] family, byte[] qualifier,
      long ts, byte[] newValue, byte[] tags) throws IOException {
    return CellUtil.createCell(row, family, qualifier, ts, KeyValue.Type.Put,
        newValue, tags);
  }

  /**
   * 创建HBase Tag。
   * @param tagType 标签类型
   * @param tag 标签内容字节数组
   * @return HBase Tag实例
   */
  public static Tag createTag(byte tagType, byte[] tag) {
    return new ArrayBackedTag(tagType, tag);
  }

  /**
   * 创建HBase Tag。
   * @param tagType 标签类型
   * @param tag 标签内容字符串
   * @return HBase Tag实例
   */
  public static Tag createTag(byte tagType, String tag) {
    return createTag(tagType, Bytes.toBytes(tag));
  }

  /**
   * 从Cell中提取标签列表。
   * @param cell 输入Cell
   * @return Cell中的标签列表
   */
  public static List<Tag> convertCellAsTagList(Cell cell) {
    return TagUtil.asList(
        cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
  }

  /**
   * 将标签列表转换为字节数组。
   * @param tags 输入标签列表
   * @return 标签列表的字节数组表示
   */
  public static byte[] convertTagListToByteArray(List<Tag> tags) {
    return TagUtil.fromList(tags);
  }

  /**
   * 从标签列表中提取聚合压缩维度应用ID。
   *
   * @param tags 输入标签列表
   * @return 应用ID，如果不存在则返回null
   */
  public static String getAggregationCompactionDimension(List<Tag> tags) {
    String appId = null;
    for (Tag t : tags) {
      if (AggregationCompactionDimension.APPLICATION_ID.getTagType() == t
          .getType()) {
        appId = Bytes.toString(Tag.cloneValue(t));
        return appId;
      }
    }
    return appId;
  }

  /**
   * 从标签列表中获取第一个匹配的聚合操作。
   *
   * @param tags HBase标签列表
   * @return 第一个匹配的聚合操作，不存在则返回null
   */
  public static AggregationOperation getAggregationOperationFromTagsList(
      List<Tag> tags) {
    for (AggregationOperation aggOp : AggregationOperation.values()) {
      for (Tag tag : tags) {
        if (tag.getType() == aggOp.getTagType()) {
          return aggOp;
        }
      }
    }
    return null;
  }

  /**
   * 刷写并压实指定表的所有Region。
   * @param server HBase RegionServer实例
   * @param table 目标表名
   * @throws IOException IO异常抛出
   * @return 处理的Region数量
   */
  public static int flushCompactTableRegions(HRegionServer server,
      TableName table) throws IOException {
    List<HRegion> regions = server.getRegions(table);
    for (HRegion region : regions) {
      region.flush(true);
      region.compact(true);
    }
    return regions.size();
  }

  /**
   * 验证表所有Region是否正确加载FlowRunCoprocessor协处理器。
   * @param server HBase RegionServer实例
   * @param table 目标表名
   * @param existenceExpected 期望协处理器是否存在：true表示应该存在，false表示应该不存在
   * @throws Exception 验证不通过抛出异常
   */
  public static void validateFlowRunCoprocessor(HRegionServer server,
      TableName table, boolean existenceExpected) throws Exception {
    List<HRegion> regions = server.getRegions(table);
    for (HRegion region : regions) {
      boolean found = false;
      Set<String> coprocs = region.getCoprocessorHost().getCoprocessors();
      for (String coprocName : coprocs) {
        if (coprocName.contains("FlowRunCoprocessor")) {
          found = true;
        }
      }
      if (found != existenceExpected) {
        throw new Exception("FlowRunCoprocessor is" +
            (existenceExpected ? " not " : " ") + "loaded in table " + table);
      }
    }
  }
}