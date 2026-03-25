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
package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.AppIdKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 实体表HBase行键封装类，存储时间线服务实体的完整行键信息，支持编码解码为字节数组或字符串。
 */
public class EntityRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final Long flowRunId;
  private final String appId;
  private final String entityType;
  private final Long entityIdPrefix;
  private final String entityId;
  private final EntityRowKeyConverter entityRowKeyConverter =
      new EntityRowKeyConverter();

  /**
   * 构造实体行键对象，传入所有层级的标识信息。
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 工作流名称
   * @param flowRunId 工作流运行ID
   * @param appId 应用ID
   * @param entityType 实体类型
   * @param entityIdPrefix 实体ID前缀
   * @param entityId 实体ID
   */
  public EntityRowKey(String clusterId, String userId, String flowName,
      Long flowRunId, String appId, String entityType, Long entityIdPrefix,
      String entityId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowRunId = flowRunId;
    this.appId = appId;
    this.entityType = entityType;
    this.entityIdPrefix = entityIdPrefix;
    this.entityId = entityId;
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

  public String getEntityType() {
    return entityType;
  }

  public String getEntityId() {
    return entityId;
  }

  public Long getEntityIdPrefix() {
    return entityIdPrefix;
  }

  /**
   * 生成实体表的HBase字节数组行键。
   * @return 编码后的行键字节数组
   */
  public byte[] getRowKey() {
    return entityRowKeyConverter.encode(this);
  }

  /**
   * 从字节数组行键解析出EntityRowKey对象。
   * @param rowKey 字节数组格式行键
   * @return 解析后的EntityRowKey对象
   */
  public static EntityRowKey parseRowKey(byte[] rowKey) {
    return new EntityRowKeyConverter().decode(rowKey);
  }

  /**
   * 生成字符串格式的实体行键，用于查询展示。
   * @return 字符串格式行键
   */
  public String getRowKeyAsString() {
    return entityRowKeyConverter.encodeAsString(this);
  }

  /**
   * 从编码字符串解析出EntityRowKey对象。
   * @param encodedRowKey 字符串格式行键
   * @return 解析后的EntityRowKey对象
   */
  public static EntityRowKey parseRowKeyFromString(String encodedRowKey) {
    return new EntityRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * 实体表行键编码器/解码器，实现字节数组和字符串与EntityRowKey对象的相互转换。
   * 行键格式：userName!clusterId!flowName!flowRunId!appId!entityType!entityIdPrefix!entityId
   */
  final private static class EntityRowKeyConverter implements
      KeyConverter<EntityRowKey>, KeyConverterToString<EntityRowKey> {

    private final AppIdKeyConverter appIDKeyConverter = new AppIdKeyConverter();

    private EntityRowKeyConverter() {
    }

    /**
     * 定义行键各段长度，可变长度用VARIABLE_SIZE标记，用于拆分解码。
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
        AppIdKeyConverter.getKeySize(), Separator.VARIABLE_SIZE,
        Bytes.SIZEOF_LONG, Separator.VARIABLE_SIZE };

    /*
     * (non-Javadoc)
     * 将EntityRowKey对象编码为HBase字节数组行键，flowRunId反转字节序实现降序排列。
     * 支持生成前缀行键，当后续字段为null时生成带结束分隔符的前缀用于范围查询。
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(EntityRowKey rowKey) {
      // 编码用户ID，转义特殊分隔符
      byte[] user =
          Separator.encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
              Separator.QUALIFIERS);
      // 编码集群ID，转义特殊分隔符
      byte[] cluster =
          Separator.encode(rowKey.getClusterId(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      // 编码工作流名称，转义特殊分隔符
      byte[] flow =
          Separator.encode(rowKey.getFlowName(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      // 拼接前三段
      byte[] first = Separator.QUALIFIERS.join(user, cluster, flow);
      // 反转flowRunId字节序，实现降序排列
      byte[] second =
          Bytes.toBytes(LongConverter.invertLong(rowKey.getFlowRunId()));
      // 编码应用ID
      byte[] third = appIDKeyConverter.encode(rowKey.getAppId());
      // 实体类型为空，返回应用前缀行键
      if (rowKey.getEntityType() == null) {
        return Separator.QUALIFIERS.join(first, second, third,
            Separator.EMPTY_BYTES);
      }
      // 编码实体类型，转义特殊分隔符
      byte[] entityType =
          Separator.encode(rowKey.getEntityType(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);

      // 实体ID前缀为空，返回实体类型前缀行键
      if (rowKey.getEntityIdPrefix() == null) {
        return Separator.QUALIFIERS.join(first, second, third, entityType,
            Separator.EMPTY_BYTES);
      }

      // 转换实体ID前缀为字节数组
      byte[] entityIdPrefix = Bytes.toBytes(rowKey.getEntityIdPrefix());

      // 实体ID为空，返回实体ID前缀前缀行键
      if (rowKey.getEntityId() == null) {
        return Separator.QUALIFIERS.join(first, second, third, entityType,
            entityIdPrefix, Separator.EMPTY_BYTES);
      }

      // 编码实体ID，转义特殊分隔符
      byte[] entityId = Separator.encode(rowKey.getEntityId(), Separator.SPACE,
          Separator.TAB, Separator.QUALIFIERS);

      // 拼接后三段
      byte[] fourth =
          Separator.QUALIFIERS.join(entityType, entityIdPrefix, entityId);

      // 返回完整行键
      return Separator.QUALIFIERS.join(first, second, third, fourth);
    }

    /*
     * (non-Javadoc)
     * 将字节数组格式行键解码为EntityRowKey对象，反转flowRunId字节序还原原始值。
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#decode(byte[])
     */
    @Override
    public EntityRowKey decode(byte[] rowKey) {
      // 按预定义段大小拆分字节行键
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      // 段数不正确抛出异常
      if (rowKeyComponents.length != 8) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "an entity");
      }
      // 解码用户ID，还原转义的特殊字符
      String userId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 解码集群ID，还原转义的特殊字符
      String clusterId =
          Separator.decode(Bytes.toString(rowKeyComponents[1]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 解码工作流名称，还原转义的特殊字符
      String flowName =
          Separator.decode(Bytes.toString(rowKeyComponents[2]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 反转字节序还原flowRunId
      Long flowRunId =
          LongConverter.invertLong(Bytes.toLong(rowKeyComponents[3]));
      // 解码应用ID
      String appId = appIDKeyConverter.decode(rowKeyComponents[4]);
      // 解码实体类型，还原转义的特殊字符
      String entityType =
          Separator.decode(Bytes.toString(rowKeyComponents[5]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);

      // 解析实体ID前缀
      Long entityPrefixId = Bytes.toLong(rowKeyComponents[6]);

      // 解码实体ID，还原转义的特殊字符
      String entityId =
          Separator.decode(Bytes.toString(rowKeyComponents[7]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 构造并返回EntityRowKey对象
      return new EntityRowKey(clusterId, userId, flowName, flowRunId, appId,
          entityType, entityPrefixId, entityId);
    }

    /**
     * 将EntityRowKey编码为可打印字符串，用于查询和展示。
     * @param key 实体行键对象
     * @return 编码后的字符串行键
     */
    @Override
    public String encodeAsString(EntityRowKey key) {
      // 所有字段必须非空才能生成完整字符串行键
      if (key.clusterId == null || key.userId == null || key.flowName == null
          || key.flowRunId == null || key.appId == null
          || key.entityType == null || key.entityIdPrefix == null
          || key.entityId == null) {
        throw new IllegalArgumentException();
      }
      // 转义并拼接所有字段
      return TimelineReaderUtils
          .joinAndEscapeStrings(new String[] {key.clusterId, key.userId,
              key.flowName, key.flowRunId.toString(), key.appId, key.entityType,
              key.entityIdPrefix.toString(), key.entityId});
    }

    /**
     * 从字符串行键解码为EntityRowKey对象。
     * @param encodedRowKey 编码后的字符串行键
     * @return 解码后的实体行键对象
     */
    @Override
    public EntityRowKey decodeFromString(String encodedRowKey) {
      // 拆分字符串行键
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      // 分段数量不正确抛出异常
      if (split == null || split.size() != 8) {
        throw new IllegalArgumentException("Invalid row key for entity table.");
      }
      // 解析flowRunId为Long类型
      Long flowRunId = Long.valueOf(split.get(3));
      // 解析实体ID前缀为Long类型
      Long entityIdPrefix = Long.valueOf(split.get(6));
      // 构造并返回EntityRowKey对象
      return new EntityRowKey(split.get(0), split.get(1), split.get(2),
          flowRunId, split.get(4), split.get(5), entityIdPrefix, split.get(7));
    }
  }
}