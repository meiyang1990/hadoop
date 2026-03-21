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

/**
 * 字符串类型键的编解码器，用于HBase中列名/行键的编码解码。
 * 编码时不会包含列前缀，列前缀会在关联的ColumnPrefix实现中按需添加。
 */
public final class StringKeyConverter implements KeyConverter<String> {

  /**
   * 构造字符串键编解码器实例。
   */
  public StringKeyConverter() {
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(String key) {
    // 对字符串键进行转义，处理空格和tab特殊字符
    return Separator.encode(key, Separator.SPACE, Separator.TAB);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public String decode(byte[] bytes) {
    // 对字节数组进行解码，还原转义的空格和tab特殊字符
    return Separator.decode(bytes, Separator.TAB, Separator.SPACE);
  }
}