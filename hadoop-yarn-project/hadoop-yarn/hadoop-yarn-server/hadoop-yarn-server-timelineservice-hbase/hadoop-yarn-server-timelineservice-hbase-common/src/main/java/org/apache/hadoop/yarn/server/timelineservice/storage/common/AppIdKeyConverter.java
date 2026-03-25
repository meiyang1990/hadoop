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
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * 为HBase行键编码/解码ApplicationId的转换器。
 * App ID在HBase行键中存储为12字节：先存集群时间戳（long类型占8字节），再存序列号（int类型占4字节）。
 * 使用位反转实现应用ID按时间降序排列，最新的应用排在最前面。
 */
public final class AppIdKeyConverter implements KeyConverter<String> {

  public AppIdKeyConverter() {
  }

  /*
   * (non-Javadoc)
   *
   * Converts/encodes a string app Id into a byte representation for (row) keys.
   * For conversion, we extract cluster timestamp and sequence id from the
   * string app id (calls ConverterUtils#toApplicationId(String) for
   * conversion) and then store it in a byte array of length 12 (8 bytes (long)
   * for cluster timestamp followed 4 bytes(int) for sequence id). Both cluster
   * timestamp and sequence id are inverted so that the most recent cluster
   * timestamp and highest sequence id appears first in the table (i.e.
   * application id appears in a descending order).
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(String appIdStr) {
    // 从字符串解析出ApplicationId对象
    ApplicationId appId = ApplicationId.fromString(appIdStr);
    // 创建长度为12字节的编码结果数组
    byte[] appIdBytes = new byte[getKeySize()];
    // 反转集群时间戳，实现降序排列，转换为字节数组
    byte[] clusterTs = Bytes.toBytes(
        LongConverter.invertLong(appId.getClusterTimestamp()));
    // 将时间戳字节拷贝到结果数组前8字节位置
    System.arraycopy(clusterTs, 0, appIdBytes, 0, Bytes.SIZEOF_LONG);
    // 反转应用序列号，实现降序排列，转换为字节数组
    byte[] seqId = Bytes.toBytes(
        HBaseTimelineSchemaUtils.invertInt(appId.getId()));
    // 将序列号字节拷贝到结果数组后4字节位置
    System.arraycopy(seqId, 0, appIdBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT);
    return appIdBytes;
  }

  /*
   * (non-Javadoc)
   *
   * Converts/decodes a 12 byte representation of app id for (row) keys to an
   * app id in string format which can be returned back to client.
   * For decoding, 12 bytes are interpreted as 8 bytes of inverted cluster
   * timestamp(long) followed by 4 bytes of inverted sequence id(int). Calls
   * ApplicationId#toString to generate string representation of app id.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public String decode(byte[] appIdBytes) {
    // 校验字节数组长度是否符合预期
    if (appIdBytes.length != getKeySize()) {
      throw new IllegalArgumentException("Invalid app id in byte format");
    }
    // 从字节数组解析出反转后的时间戳，再反转还原得到原始值
    long clusterTs = LongConverter.invertLong(
        Bytes.toLong(appIdBytes, 0, Bytes.SIZEOF_LONG));
    // 从字节数组解析出反转后的序列号，再反转还原得到原始值
    int seqId = HBaseTimelineSchemaUtils.invertInt(
        Bytes.toInt(appIdBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT));
    // 构造ApplicationId对象并转换为字符串返回
    return HBaseTimelineSchemaUtils.convertApplicationIdToString(
        ApplicationId.newInstance(clusterTs, seqId));
  }

  /**
   * 返回编码后App ID的字节长度。
   *
   * @return 编码后App ID的字节长度
   */
  public static int getKeySize() {
    return Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;
  }
}