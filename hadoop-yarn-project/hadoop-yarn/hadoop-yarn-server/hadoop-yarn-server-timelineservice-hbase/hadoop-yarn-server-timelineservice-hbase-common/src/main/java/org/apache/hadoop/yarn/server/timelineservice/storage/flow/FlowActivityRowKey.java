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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineSchemaUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 表示流活动HBase表的RowKey结构
 */
public class FlowActivityRowKey {

  private final String clusterId;
  private final Long dayTs;
  private final String userId;
  private final String flowName;
  private final FlowActivityRowKeyConverter
      flowActivityRowKeyConverter = new FlowActivityRowKeyConverter();

  /**
   * 构造流活动RowKey对象，自动将时间戳转换为当日零点时间
   * @param clusterId 集群标识
   * @param dayTs 原始时间戳
   * @param userId 用户标识
   * @param flowName 流名称
   */
  public FlowActivityRowKey(String clusterId, Long dayTs, String userId,
      String flowName) {
    this(clusterId, dayTs, userId, flowName, true);
  }

  /**
   * 构造流活动RowKey对象，可选择是否将时间戳转换为当日零点
   * @param clusterId 集群标识
   * @param timestamp 流活动发生时间戳
   * @param userId 用户标识
   * @param flowName 流名称
   * @param convertDayTsToTopOfDay 是否将时间戳转换为当日零点时间
   */
  protected FlowActivityRowKey(String clusterId, Long timestamp, String userId,
      String flowName, boolean convertDayTsToTopOfDay) {
    this.clusterId = clusterId;
    if (convertDayTsToTopOfDay && (timestamp != null)) {
      // 将时间戳转换为当日零点，方便按天维度聚合查询
      this.dayTs = HBaseTimelineSchemaUtils.getTopOfTheDayTimestamp(timestamp);
    } else {
      this.dayTs = timestamp;
    }
    this.userId = userId;
    this.flowName = flowName;
  }

  public String getClusterId() {
    return clusterId;
  }

  public Long getDayTimestamp() {
    return dayTs;
  }

  public String getUserId() {
    return userId;
  }

  public String getFlowName() {
    return flowName;
  }

  /**
   * 构造流活动表的HBase RowKey字节数组，格式为: clusterId!dayTimestamp!user!flowName
   *
   * @return 流活动RowKey字节数组
   */
  public byte[] getRowKey() {
    return flowActivityRowKeyConverter.encode(this);
  }

  /**
   * 从字节数组解析出流活动RowKey对象
   *
   * @param rowKey RowKey字节数组
   * @return 解析后的FlowActivityRowKey对象
   */
  public static FlowActivityRowKey parseRowKey(byte[] rowKey) {
    return new FlowActivityRowKeyConverter().decode(rowKey);
  }

  /**
   * 构造流活动RowKey的字符串表示，格式为: clusterId!dayTimestamp!user!flowName
   * @return 流活动RowKey字符串
   */
  public String getRowKeyAsString() {
    return flowActivityRowKeyConverter.encodeAsString(this);
  }

  /**
   * 从字符串解析出流活动RowKey对象
   * @param encodedRowKey RowKey字符串
   * @return 解析后的FlowActivityRowKey对象
   */
  public static FlowActivityRowKey parseRowKeyFromString(String encodedRowKey) {
    return new FlowActivityRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * 流活动RowKey的编解码器，负责在对象和HBase字节数组/字符串之间转换
   * RowKey格式为: clusterId!dayTimestamp!user!flowName，其中dayTimestamp是长整型，其余为字符串
   * <p>
   */
  final private static class FlowActivityRowKeyConverter
      implements KeyConverter<FlowActivityRowKey>,
      KeyConverterToString<FlowActivityRowKey> {

    private FlowActivityRowKeyConverter() {
    }

    /**
     * 定义每个分段的大小，用于解码时分段切割：
     * clusterId: 变长，dayTimestamp: 固定8字节(long)，userId:变长，flowName:变长
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Bytes.SIZEOF_LONG, Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE };

    /*
     * (non-Javadoc)
     *
     * 将FlowActivityRowKey对象编码为HBase RowKey字节数组，各部分使用!分隔。
     * 时间戳会被反转，使得行键在HBase中按时间降序排列，最新数据优先查询。
     * 根据字段是否为null生成不同长度的前缀，支持范围查询场景。
     *
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(FlowActivityRowKey rowKey) {
      if (rowKey.getDayTimestamp() == null) {
        // 仅返回clusterId前缀，用于查询该集群下所有流活动
        return Separator.QUALIFIERS.join(Separator.encode(
            rowKey.getClusterId(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS), Separator.EMPTY_BYTES);
      }
      if (rowKey.getUserId() == null) {
        // 返回clusterId!dayTimestamp前缀，用于查询该集群某一天下所有流活动
        return Separator.QUALIFIERS.join(Separator.encode(
            rowKey.getClusterId(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS), Bytes.toBytes(LongConverter
            .invertLong(rowKey.getDayTimestamp())), Separator.EMPTY_BYTES);
      }
      // 编码完整RowKey: clusterId!反转后的dayTimestamp!userId!flowName
      return Separator.QUALIFIERS.join(Separator.encode(rowKey.getClusterId(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS), Bytes
          .toBytes(LongConverter.invertLong(rowKey.getDayTimestamp())),
          Separator.encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
              Separator.QUALIFIERS), Separator.encode(rowKey.getFlowName(),
              Separator.SPACE, Separator.TAB, Separator.QUALIFIERS));
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#decode(byte[])
     */
    @Override
    public FlowActivityRowKey decode(byte[] rowKey) {
      // 按分隔符和分段大小切割RowKey
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      if (rowKeyComponents.length != 4) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "a flow activity");
      }
      // 解码各分段，还原转义字符
      String clusterId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 反转时间戳还原原始值
      Long dayTs = LongConverter.invertLong(Bytes.toLong(rowKeyComponents[1]));
      String userId =
          Separator.decode(Bytes.toString(rowKeyComponents[2]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String flowName =
          Separator.decode(Bytes.toString(rowKeyComponents[3]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      return new FlowActivityRowKey(clusterId, dayTs, userId, flowName);
    }

    @Override
    public String encodeAsString(FlowActivityRowKey key) {
      if (key.getDayTimestamp() == null) {
        // 仅编码clusterId
        return TimelineReaderUtils
            .joinAndEscapeStrings(new String[] {key.clusterId});
      } else if (key.getUserId() == null) {
        // 编码clusterId + 时间戳
        return TimelineReaderUtils.joinAndEscapeStrings(
            new String[] {key.clusterId, key.dayTs.toString()});
      } else if (key.getFlowName() == null) {
        // 编码clusterId + 时间戳 + userId
        return TimelineReaderUtils.joinAndEscapeStrings(
            new String[] {key.clusterId, key.dayTs.toString(), key.userId});
      }
      // 编码完整四个字段
      return TimelineReaderUtils.joinAndEscapeStrings(new String[] {
          key.clusterId, key.dayTs.toString(), key.userId, key.flowName});
    }

    @Override
    public FlowActivityRowKey decodeFromString(String encodedRowKey) {
      // 切割并反转义字符串
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      if (split == null || split.size() != 4) {
        throw new IllegalArgumentException(
            "Invalid row key for flow activity.");
      }
      Long dayTs = Long.valueOf(split.get(1));
      return new FlowActivityRowKey(split.get(0), dayTs, split.get(2),
          split.get(3));
    }
  }
}