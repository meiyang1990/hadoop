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
package org.apache.hadoop.yarn.server.timelineservice.storage.subapplication;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 子应用表的HBase行键模型，存储子应用行键各组成部分并提供编解码能力。
 */
public class SubApplicationRowKey {
  private final String subAppUserId;
  private final String clusterId;
  private final String entityType;
  private final Long entityIdPrefix;
  private final String entityId;
  private final String userId;
  private final SubApplicationRowKeyConverter subAppRowKeyConverter =
      new SubApplicationRowKeyConverter();

  /**
   * 构造子应用行键对象，保存各组成部分。
   * @param subAppUserId 子应用代理用户（通常是doAsUser）
   * @param clusterId 集群ID
   * @param entityType 实体类型
   * @param entityIdPrefix 实体ID前缀（长整型）
   * @param entityId 实体ID
   * @param userId 运行AM的YARN用户
   */
  public SubApplicationRowKey(String subAppUserId, String clusterId,
      String entityType, Long entityIdPrefix, String entityId, String userId) {
    this.subAppUserId = subAppUserId;
    this.clusterId = clusterId;
    this.entityType = entityType;
    this.entityIdPrefix = entityIdPrefix;
    this.entityId = entityId;
    this.userId = userId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getSubAppUserId() {
    return subAppUserId;
  }

  public String getEntityType() {
    return entityType;
  }

  public String getEntityId() {
    return entityId;
  }

  public Long getEntityIdPrefix() {
    return entityIdPrefix;
  }

  public String getUserId() {
    return userId;
  }

  /**
   * 构造HBase行键字节数组，用于子应用表查询。
   * @return 编码后的行键字节数组
   */
  public byte[] getRowKey() {
    return subAppRowKeyConverter.encode(this);
  }

  /**
   * 从字节数组解析子应用行键对象。
   * @param rowKey 字节格式的行键
   * @return 解析后的SubApplicationRowKey对象
   */
  public static SubApplicationRowKey parseRowKey(byte[] rowKey) {
    return new SubApplicationRowKeyConverter().decode(rowKey);
  }

  /**
   * 构造字符串格式的子应用行键。
   * @return 字符串格式的行键
   */
  public String getRowKeyAsString() {
    return subAppRowKeyConverter.encodeAsString(this);
  }

  /**
   * 从字符串解析子应用行键对象。
   * @param encodedRowKey 字符串格式的编码行键
   * @return 解析后的SubApplicationRowKey对象
   */
  public static SubApplicationRowKey parseRowKeyFromString(
      String encodedRowKey) {
    return new SubApplicationRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * 子应用行键编解码器，实现子应用行键在对象、字节数组、字符串之间的转换。
   * 行键格式：subAppUserId!clusterId!entityType!entityPrefix!entityId!userId
   */
  final private static class SubApplicationRowKeyConverter
      implements KeyConverter<SubApplicationRowKey>,
      KeyConverterToString<SubApplicationRowKey> {

    private SubApplicationRowKeyConverter() {
    }

    /**
     * 定义行键各段长度，变长字段用VARIABLE_SIZE表示，固定长记录字节数。
     * 顺序：subAppUserId、clusterId、entityType、entityIdPrefix、entityId、userId
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE};

    /*
     * (non-Javadoc)
     *
     * Encodes SubApplicationRowKey object into a byte array with each
     * component/field in SubApplicationRowKey separated by
     * Separator#QUALIFIERS.
     * This leads to an sub app table row key of the form
     * subAppUserId!clusterId!entityType!entityPrefix!entityId!userId
     *
     * subAppUserId is usually the doAsUser.
     * userId is the yarn user that the AM runs as.
     *
     * If entityType in passed SubApplicationRowKey object is null (and the
     * fields preceding it are not null i.e. clusterId, subAppUserId), this
     * returns a row key prefix of the form subAppUserId!clusterId!
     * If entityId in SubApplicationRowKey is null
     * (other components are not null), this returns a row key prefix
     * of the form subAppUserId!clusterId!entityType!
     *
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(SubApplicationRowKey rowKey) {
      // 编码子应用用户，转义特殊字符
      byte[] subAppUser = Separator.encode(rowKey.getSubAppUserId(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
      // 编码集群ID，转义特殊字符
      byte[] cluster = Separator.encode(rowKey.getClusterId(), Separator.SPACE,
          Separator.TAB, Separator.QUALIFIERS);
      // 拼接子应用用户和集群ID
      byte[] first = Separator.QUALIFIERS.join(subAppUser, cluster);
      // 如果实体类型为空，返回前缀行键用于范围扫描
      if (rowKey.getEntityType() == null) {
        return first;
      }
      // 编码实体类型，转义特殊字符
      byte[] entityType = Separator.encode(rowKey.getEntityType(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
      // 如果实体ID前缀为空，返回前缀行键用于范围扫描
      if (rowKey.getEntityIdPrefix() == null) {
        return Separator.QUALIFIERS.join(first, entityType,
            Separator.EMPTY_BYTES);
      }
      // 将实体ID前缀转换为字节数组（固定8字节长整型）
      byte[] entityIdPrefix = Bytes.toBytes(rowKey.getEntityIdPrefix());
      // 如果实体ID为空，返回前缀行键用于范围扫描
      if (rowKey.getEntityId() == null) {
        return Separator.QUALIFIERS.join(first, entityType, entityIdPrefix,
            Separator.EMPTY_BYTES);
      }
      // 编码实体ID，转义特殊字符
      byte[] entityId = Separator.encode(rowKey.getEntityId(), Separator.SPACE,
          Separator.TAB, Separator.QUALIFIERS);
      // 编码YARN用户，转义特殊字符
      byte[] userId = Separator.encode(rowKey.getUserId(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
      // 拼接后半部分字段
      byte[] second = Separator.QUALIFIERS.join(entityType, entityIdPrefix,
          entityId, userId);
      // 拼接完整行键并返回
      return Separator.QUALIFIERS.join(first, second);
    }

    /*
     * (non-Javadoc)
     *
     * Decodes a sub application row key of the form
     * subAppUserId!clusterId!entityType!entityPrefix!entityId!userId
     *
     * subAppUserId is usually the doAsUser.
     * userId is the yarn user that the AM runs as.
     *
     * represented in byte format
     * and converts it into an SubApplicationRowKey object.
     *
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#decode(byte[])
     */
    @Override
    public SubApplicationRowKey decode(byte[] rowKey) {
      // 按分隔符和段大小拆分字节行键为各部分
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      // 段数不对则抛出非法参数异常
      if (rowKeyComponents.length != 6) {
        throw new IllegalArgumentException(
            "the row key is not valid for " + "a sub app");
      }
      // 解码第一段：子应用用户，反转义特殊字符
      String subAppUserId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 解码第二段：集群ID，反转义特殊字符
      String clusterId = Separator.decode(Bytes.toString(rowKeyComponents[1]),
          Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 解码第三段：实体类型，反转义特殊字符
      String entityType = Separator.decode(Bytes.toString(rowKeyComponents[2]),
          Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 解码第四段：实体ID前缀，转换为长整型
      Long entityPrefixId = Bytes.toLong(rowKeyComponents[3]);
      // 解码第五段：实体ID，反转义特殊字符
      String entityId = Separator.decode(Bytes.toString(rowKeyComponents[4]),
          Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 解码第六段：YARN用户，反转义特殊字符
      String userId =
          Separator.decode(Bytes.toString(rowKeyComponents[5]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 构造并返回行键对象
      return new SubApplicationRowKey(subAppUserId, clusterId, entityType,
          entityPrefixId, entityId, userId);
    }

    @Override
    public String encodeAsString(SubApplicationRowKey key) {
      // 检查所有字段非空，否则抛出异常
      if (key.subAppUserId == null || key.clusterId == null
          || key.entityType == null || key.entityIdPrefix == null
          || key.entityId == null || key.userId == null) {
        throw new IllegalArgumentException();
      }
      // 转义并拼接各字段为字符串行键
      return TimelineReaderUtils.joinAndEscapeStrings(
          new String[] {key.subAppUserId, key.clusterId, key.entityType,
              key.entityIdPrefix.toString(), key.entityId, key.userId});
    }

    @Override
    public SubApplicationRowKey decodeFromString(String encodedRowKey) {
      // 拆分字符串行键为各部分
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      // 段数不对则抛出异常
      if (split == null || split.size() != 6) {
        throw new IllegalArgumentException(
            "Invalid row key for sub app table.");
      }
      // 将第四段转换为长整型实体ID前缀
      Long entityIdPrefix = Long.valueOf(split.get(3));
      // 构造并返回行键对象
      return new SubApplicationRowKey(split.get(0), split.get(1),
          split.get(2), entityIdPrefix, split.get(4), split.get(5));
    }
  }
}