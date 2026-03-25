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

package org.apache.hadoop.yarn.server.timelineservice.storage.application;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.AppIdKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 应用表HBase行键表示类，封装应用行键各组成部分及编解码逻辑。
 * Represents a rowkey for the application table.
 */
public class ApplicationRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final Long flowRunId;
  private final String appId;
  private final ApplicationRowKeyConverter appRowKeyConverter =
      new ApplicationRowKeyConverter();

  /**
   * 构造应用行键对象，传入各组成部分。
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 流名称
   * @param flowRunId 流运行ID
   * @param appId 应用ID
   */
  public ApplicationRowKey(String clusterId, String userId, String flowName,
      Long flowRunId, String appId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowRunId = flowRunId;
    this.appId = appId;
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

  public String getAppId() {
    return appId;
  }

  /**
   * 生成应用表HBase行键字节数组。
   * Constructs a row key for the application table as follows:
   * {@code clusterId!userName!flowName!flowRunId!AppId}.
   *
   * @return byte array with the row key
   */
  public byte[] getRowKey() {
    return appRowKeyConverter.encode(this);
  }

  /**
   * 从字节数组解析应用行键对象。
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey Byte representation of row key.
   * @return An <cite>ApplicationRowKey</cite> object.
   */
  public static ApplicationRowKey parseRowKey(byte[] rowKey) {
    return new ApplicationRowKeyConverter().decode(rowKey);
  }

  /**
   * 生成应用行键字符串表示。
   * Constructs a row key for the application table as follows:
   * {@code clusterId!userName!flowName!flowRunId!AppId}.
   * @return String representation of row key.
   */
  public String getRowKeyAsString() {
    return appRowKeyConverter.encodeAsString(this);
  }

  /**
   * 从编码字符串解析应用行键对象。
   * Given the encoded row key as string, returns the row key as an object.
   * @param encodedRowKey String representation of row key.
   * @return A <cite>ApplicationRowKey</cite> object.
   */
  public static ApplicationRowKey parseRowKeyFromString(String encodedRowKey) {
    return new ApplicationRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * 应用表行键编解码转换器，实现字节数组和字符串的编解码逻辑。
   * Encodes and decodes row key for application table. The row key is of the
   * form: clusterId!userName!flowName!flowRunId!appId. flowRunId is a long,
   * appId is encoded and decoded using {@link AppIdKeyConverter} and rest are
   * strings.
   * <p>
   */
  final private static class ApplicationRowKeyConverter implements
      KeyConverter<ApplicationRowKey>, KeyConverterToString<ApplicationRowKey> {

    private final KeyConverter<String> appIDKeyConverter =
        new AppIdKeyConverter();

    /**
     * Intended for use in ApplicationRowKey only.
     */
    private ApplicationRowKeyConverter() {
    }

    /**
     * 行键各段大小定义，用于解码时分段切割：前三个段为变长字符串，flowRunId定长8字节，appId定长12字节。
     * Application row key is of the form
     * clusterId!userName!flowName!flowRunId!appId with each segment separated
     * by !. The sizes below indicate sizes of each one of these segements in
     * sequence. clusterId, userName and flowName are strings. flowrunId is a
     * long hence 8 bytes in size. app id is represented as 12 bytes with
     * cluster timestamp part of appid takes 8 bytes(long) and seq id takes 4
     * bytes(int). Strings are variable in size (i.e. end whenever separator is
     * encountered). This is used while decoding and helps in determining where
     * to split.
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
        AppIdKeyConverter.getKeySize() };

    /*
     * (non-Javadoc)
     *
     * Encodes ApplicationRowKey object into a byte array with each
     * component/field in ApplicationRowKey separated by Separator#QUALIFIERS.
     * This leads to an application table row key of the form
     * clusterId!userName!flowName!flowRunId!appId If flowRunId in passed
     * ApplicationRowKey object is null (and the fields preceding it i.e.
     * clusterId, userId and flowName are not null), this returns a row key
     * prefix of the form clusterId!userName!flowName! and if appId in
     * ApplicationRowKey is null (other 4 components all are not null), this
     * returns a row key prefix of the form
     * clusterId!userName!flowName!flowRunId! flowRunId is inverted while
     * encoding as it helps maintain a descending order for row keys in the
     * application table.
     *
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(ApplicationRowKey rowKey) {
      // 编码集群ID，转义特殊字符
      byte[] cluster =
          Separator.encode(rowKey.getClusterId(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      // 编码用户ID，转义特殊字符
      byte[] user =
          Separator.encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
              Separator.QUALIFIERS);
      // 编码流名称，转义特殊字符
      byte[] flow =
          Separator.encode(rowKey.getFlowName(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      // 拼接前三个分段
      byte[] first = Separator.QUALIFIERS.join(cluster, user, flow);
      // flowRunId为null，返回前缀匹配用行键
      if (rowKey.getFlowRunId() == null) {
        return Separator.QUALIFIERS.join(first, Separator.EMPTY_BYTES);
      }
      // 反转flowRunId数值，保证行键按降序排列存储
      byte[] second =
          Bytes.toBytes(LongConverter.invertLong(
              rowKey.getFlowRunId()));
      // 应用ID为空，返回前缀匹配用行键
      if (rowKey.getAppId() == null || rowKey.getAppId().isEmpty()) {
        return Separator.QUALIFIERS.join(first, second, Separator.EMPTY_BYTES);
      }
      // 编码应用ID
      byte[] third = appIDKeyConverter.encode(rowKey.getAppId());
      // 拼接完整行键并返回
      return Separator.QUALIFIERS.join(first, second, third);
    }

    /*
     * (non-Javadoc)
     *
     * Decodes an application row key of the form
     * clusterId!userName!flowName!flowRunId!appId represented in byte format
     * and converts it into an ApplicationRowKey object.flowRunId is inverted
     * while decoding as it was inverted while encoding.
     *
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#decode(byte[])
     */
    @Override
    public ApplicationRowKey decode(byte[] rowKey) {
      // 根据分段大小拆分出行键各部分字节
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      // 分段数量不正确，抛出非法参数异常
      if (rowKeyComponents.length != 5) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "an application");
      }
      // 解码集群ID，反转义特殊字符
      String clusterId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 解码用户ID，反转义特殊字符
      String userId =
          Separator.decode(Bytes.toString(rowKeyComponents[1]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 解码流名称，反转义特殊字符
      String flowName =
          Separator.decode(Bytes.toString(rowKeyComponents[2]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 解码flowRunId，反转数值恢复原始值
      Long flowRunId =
          LongConverter.invertLong(Bytes.toLong(rowKeyComponents[3]));
      // 解码应用ID
      String appId = appIDKeyConverter.decode(rowKeyComponents[4]);
      // 构造并返回应用行键对象
      return new ApplicationRowKey(clusterId, userId, flowName, flowRunId,
          appId);
    }

    /**
     * 将应用行键编码为字符串，供时间线读取使用。
     */
    @Override
    public String encodeAsString(ApplicationRowKey key) {
      // 任意组成部分为空，抛出非法参数异常
      if (key.clusterId == null || key.userId == null || key.flowName == null
          || key.flowRunId == null || key.appId == null) {
        throw new IllegalArgumentException();
      }
      // 转义并拼接各部分为字符串
      return TimelineReaderUtils
          .joinAndEscapeStrings(new String[] {key.clusterId, key.userId,
              key.flowName, key.flowRunId.toString(), key.appId});
    }

    /**
     * 从编码字符串解码出应用行键对象。
     */
    @Override
    public ApplicationRowKey decodeFromString(String encodedRowKey) {
      // 拆分字符串得到各组成部分
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      // 拆分结果不合法，抛出异常
      if (split == null || split.size() != 5) {
        throw new IllegalArgumentException(
            "Invalid row key for application table.");
      }
      // 解析flowRunId为长整型
      Long flowRunId = Long.valueOf(split.get(3));
      // 构造并返回应用行键对象
      return new ApplicationRowKey(split.get(0), split.get(1), split.get(2),
          flowRunId, split.get(4));
    }
  }

}