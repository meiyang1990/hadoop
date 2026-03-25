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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 表示流运行表(HBase)的RowKey结构，封装了流运行RowKey的各个组成部分与编解码逻辑
 */
public class FlowRunRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final Long flowRunId;
  private final FlowRunRowKeyConverter flowRunRowKeyConverter =
      new FlowRunRowKeyConverter();

  /**
   * 构造流运行RowKey对象
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 流名称
   * @param flowRunId 流运行ID
   */
  public FlowRunRowKey(String clusterId, String userId, String flowName,
      Long flowRunId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowRunId = flowRunId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getUserId() {
    return userId;
  }

  public String getFlowName() {
    return flowName;
  }

  public Long getFlowRunId() {
    return flowRunId;
  }

  /**
   * 构造HBase存储用的字节数组RowKey
   * @return 字节数组形式的RowKey
   */
  public byte[] getRowKey() {
    return flowRunRowKeyConverter.encode(this);
  }


  /**
   * 从字节数组解析出流运行RowKey对象
   * @param rowKey 字节形式的RowKey
   * @return 解析后的FlowRunRowKey对象
   */
  public static FlowRunRowKey parseRowKey(byte[] rowKey) {
    return new FlowRunRowKeyConverter().decode(rowKey);
  }

  /**
   * 获取字符串形式的RowKey
   * @return 字符串形式的RowKey
   */
  public String getRowKeyAsString() {
    return flowRunRowKeyConverter.encodeAsString(this);
  }

  /**
   * 从字符串解析出流运行RowKey对象
   * @param encodedRowKey 字符串形式的RowKey
   * @return 解析后的FlowRunRowKey对象
   */
  public static FlowRunRowKey parseRowKeyFromString(String encodedRowKey) {
    return new FlowRunRowKeyConverter().decodeFromString(encodedRowKey);
  }

  @Override
  public String toString() {
    StringBuilder flowKeyStr = new StringBuilder();
    flowKeyStr.append("{clusterId=" + clusterId)
        .append(" userId=" + userId)
        .append(" flowName=" + flowName)
        .append(" flowRunId=")
        .append(flowRunId)
        .append("}");
    return flowKeyStr.toString();
  }

  /**
   * 流运行RowKey编解码器，实现字节数组/字符串与FlowRunRowKey对象的互相转换
   */
  final private static class FlowRunRowKeyConverter implements
      KeyConverter<FlowRunRowKey>, KeyConverterToString<FlowRunRowKey> {

    private FlowRunRowKeyConverter() {
    }

    /**
     * 各段大小定义：前三个字段是变长字符串，最后flowRunId是固定8字节long
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG };

    /**
     * 将FlowRunRowKey对象编码为字节数组RowKey
     * 格式为 clusterId!userId!flowName!反转后的flowRunId
     * 反转flowRunId实现降序排列，保证新运行排在前面
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(FlowRunRowKey rowKey) {
      // 编码前三个字符串字段并拼接
      byte[] first =
          Separator.QUALIFIERS.join(Separator.encode(rowKey.getClusterId(),
              Separator.SPACE, Separator.TAB, Separator.QUALIFIERS), Separator
              .encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
                  Separator.QUALIFIERS), Separator.encode(rowKey.getFlowName(),
              Separator.SPACE, Separator.TAB, Separator.QUALIFIERS));
      // flowRunId为空时返回前缀，用于前缀扫描
      if (rowKey.getFlowRunId() == null) {
        return Separator.QUALIFIERS.join(first, Separator.EMPTY_BYTES);
      } else {
        // 反转flowRunId实现降序，转换为字节后拼接
        byte[] second =
            Bytes.toBytes(LongConverter.invertLong(rowKey.getFlowRunId()));
        return Separator.QUALIFIERS.join(first, second);
      }
    }

    /**
     * 将字节数组RowKey解码为FlowRunRowKey对象
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter#decode(byte[])
     */
    @Override
    public FlowRunRowKey decode(byte[] rowKey) {
      // 按分隔符和段大小拆分RowKey
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      if (rowKeyComponents.length != 4) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "a flow run");
      }
      // 解码前三个字符串字段
      String clusterId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String userId =
          Separator.decode(Bytes.toString(rowKeyComponents[1]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String flowName =
          Separator.decode(Bytes.toString(rowKeyComponents[2]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 反转还原得到原始flowRunId
      Long flowRunId =
          LongConverter.invertLong(Bytes.toLong(rowKeyComponents[3]));
      return new FlowRunRowKey(clusterId, userId, flowName, flowRunId);
    }

    /**
     * 将FlowRunRowKey编码为字符串形式
     */
    @Override
    public String encodeAsString(FlowRunRowKey key) {
      if (key.clusterId == null || key.userId == null || key.flowName == null
          || key.flowRunId == null) {
        throw new IllegalArgumentException();
      }
      // 拼接并转义各字段
      return TimelineReaderUtils.joinAndEscapeStrings(new String[] {
          key.clusterId, key.userId, key.flowName, key.flowRunId.toString()});
    }

    /**
     * 从字符串解析出FlowRunRowKey对象
     */
    @Override
    public FlowRunRowKey decodeFromString(String encodedRowKey) {
      // 拆分字符串，处理转义
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      if (split == null || split.size() != 4) {
        throw new IllegalArgumentException(
            "Invalid row key for flow run table.");
      }
      Long flowRunId = Long.valueOf(split.get(3));
      return new FlowRunRowKey(split.get(0), split.get(1), split.get(2),
          flowRunId);
    }
  }
}