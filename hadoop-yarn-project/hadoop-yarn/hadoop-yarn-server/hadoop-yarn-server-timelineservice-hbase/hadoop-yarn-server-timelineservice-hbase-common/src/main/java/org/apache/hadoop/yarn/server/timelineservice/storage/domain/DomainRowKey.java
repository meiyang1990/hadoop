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
package org.apache.hadoop.yarn.server.timelineservice.storage.domain;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;

/**
 * 表示HBase中domain表的行键，结构为 clusterId!domainId。
 */
public class DomainRowKey {
  private final String clusterId;
  private final String domainId;
  private final DomainRowKeyConverter domainIdKeyConverter =
      new DomainRowKeyConverter();

  /**
   * 构造domain表行键对象。
   * @param clusterId 集群ID
   * @param domainId 域ID
   */
  public DomainRowKey(String clusterId, String domainId) {
    this.clusterId = clusterId;
    this.domainId = domainId;
  }


  public String getClusterId() {
    return clusterId;
  }

  public String getDomainId() {
    return domainId;
  }

  /**
   * 构造domain表的行键字节数组。
   *
   * @return 行键对应的字节数组
   */
  public  byte[] getRowKey() {

    return domainIdKeyConverter.encode(this);
  }

  /**
   * 从字节数组解析出DomainRowKey对象。
   *
   * @param rowKey 行键字节数组
   * @return 解析后的DomainRowKey对象
   */
  public static DomainRowKey parseRowKey(byte[] rowKey) {
    return new DomainRowKeyConverter().decode(rowKey);
  }

  /**
   * 获取行键的字符串表示，格式为 clusterId!domainId。
   * @return 行键字符串
   */
  public String getRowKeyAsString() {
    return domainIdKeyConverter.encodeAsString(this);
  }

  /**
   * 从字符串解析出DomainRowKey对象。
   * @param encodedRowKey 编码后的行键字符串
   * @return 解析后的DomainRowKey对象
   */
  public static DomainRowKey parseRowKeyFromString(String encodedRowKey) {
    return new DomainRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * 负责DomainRowKey的编码与解码，实现行键对象与字节数组/字符串的互相转换。
   */
  final private static class DomainRowKeyConverter
      implements KeyConverter<DomainRowKey>,
      KeyConverterToString<DomainRowKey> {

    private DomainRowKeyConverter() {
    }

    /**
     * 各分段大小标记，两个分段都是可变长度（遇到分隔符截止），用于解码时分段切分。
     */
    private static final int[] SEGMENT_SIZES = {
        Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE};

    /*
     * (non-Javadoc)
     *
     * 将DomainRowKey对象编码为字节数组
     *
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(DomainRowKey rowKey) {
      if (rowKey == null) {
        return Separator.EMPTY_BYTES;
      }
      // 转义clusterId中的特殊字符
      byte[] cluster =
          Separator.encode(rowKey.getClusterId(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      // 转义domainId中的特殊字符
      byte[] domainIdBytes =
          Separator.encode(rowKey.getDomainId(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      // 使用分隔符拼接得到最终行键字节数组
      return Separator.QUALIFIERS.join(cluster, domainIdBytes);
    }

    @Override
    public DomainRowKey decode(byte[] rowKey) {
      // 按分隔符切分行键得到两个分段
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      if (rowKeyComponents.length != 2) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "a domain id");
      }
      // 还原clusterId中的转义字符
      String clusterId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      // 还原domainId中的转义字符
      String domainId =
          Separator.decode(Bytes.toString(rowKeyComponents[1]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);

      return new DomainRowKey(clusterId, domainId);
    }

    @Override
    public String encodeAsString(DomainRowKey key) {
      // 拼接并转义得到行键字符串
      return TimelineReaderUtils.joinAndEscapeStrings(
          new String[] {key.clusterId, key.domainId});
    }

    @Override
    public DomainRowKey decodeFromString(String encodedRowKey) {
      // 拆分编码后的行键字符串
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      if (split == null || split.size() != 2) {
        throw new IllegalArgumentException(
            "Invalid row key for domain id.");
      }
      return new DomainRowKey(split.get(0), split.get(1));
    }
  }
}