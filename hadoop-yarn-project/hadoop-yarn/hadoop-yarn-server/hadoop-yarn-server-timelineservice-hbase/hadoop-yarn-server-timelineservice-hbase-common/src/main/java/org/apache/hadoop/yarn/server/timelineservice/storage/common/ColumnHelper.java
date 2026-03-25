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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase存储时序数据列名工具类，为具体列类型构造组合列限定符，不供客户端直接使用。
 */
public final class ColumnHelper {

  private ColumnHelper() {
  }


  /**
   * 根据列前缀和字符串限定符拼接生成完整的HBase列限定符。
   * @param columnPrefixBytes 列前缀的字节表示，不应包含限定符分隔符
   * @param qualifier 列限定符后缀，允许包含限定符分隔符
   * @return 组合后的完整列限定符，若前缀为null则直接返回编码后的限定符
   */
  public static byte[] getColumnQualifier(byte[] columnPrefixBytes,
      String qualifier) {

    // 对限定符编码，移除空格和制表符
    byte[] encodedQualifier =
        Separator.encode(qualifier, Separator.SPACE, Separator.TAB);
    if (columnPrefixBytes == null) {
      return encodedQualifier;
    }

    // 使用分隔符拼接列前缀和编码后的限定符
    byte[] columnQualifier =
        Separator.QUALIFIERS.join(columnPrefixBytes, encodedQualifier);
    return columnQualifier;
  }

  /**
   * 根据列前缀和长整型限定符拼接生成完整的HBase列限定符。
   * @param columnPrefixBytes 列前缀的字节表示，不应包含限定符分隔符
   * @param qualifier 长整型列限定符后缀
   * @return 组合后的完整列限定符，若前缀为null则直接返回编码后的限定符
   */
  public static byte[] getColumnQualifier(byte[] columnPrefixBytes,
      long qualifier) {

    if (columnPrefixBytes == null) {
      return Bytes.toBytes(qualifier);
    }

    // 使用分隔符拼接列前缀和转换后的限定符字节
    byte[] columnQualifier =
        Separator.QUALIFIERS.join(columnPrefixBytes, Bytes.toBytes(qualifier));
    return columnQualifier;
  }

  /**
   * 根据列前缀和字节数组限定符拼接生成完整的HBase列限定符。
   * @param columnPrefixBytes 列前缀的字节表示，不应包含限定符分隔符
   * @param qualifier 字节数组形式的列限定符后缀
   * @return 组合后的完整列限定符，若前缀为null则直接返回输入限定符
   */
  public static byte[] getColumnQualifier(byte[] columnPrefixBytes,
      byte[] qualifier) {

    if (columnPrefixBytes == null) {
      return qualifier;
    }

    // 使用分隔符拼接列前缀和限定符字节
    byte[] columnQualifier =
        Separator.QUALIFIERS.join(columnPrefixBytes, qualifier);
    return columnQualifier;
  }

}