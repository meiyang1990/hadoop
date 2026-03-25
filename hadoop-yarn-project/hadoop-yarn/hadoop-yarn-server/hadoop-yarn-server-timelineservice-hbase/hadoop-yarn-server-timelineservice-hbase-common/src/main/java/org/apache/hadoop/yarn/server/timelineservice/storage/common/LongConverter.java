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

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Long类型与HBase字节数组的转换器，实现数值编解码、比较、加法运算，支持时间戳反转排序。
 * 用于YARN时间线服务HBase存储层的数值处理。
 */
public final class LongConverter implements NumericValueConverter,
    Serializable {

  /**
   * 序列化版本ID，因实现比较器接口需要。
   */
  private static final long serialVersionUID = 1L;

  /**
   * 默认构造函数。
   */
  public LongConverter() {
  }

  @Override
  public byte[] encodeValue(Object value) throws IOException {
    // 检查输入是否为整数类型
    if (!HBaseTimelineSchemaUtils.isIntegralValue(value)) {
      throw new IOException("Expected integral value");
    }
    // 将整数转换为HBase字节数组
    return Bytes.toBytes(((Number)value).longValue());
  }

  @Override
  public Object decodeValue(byte[] bytes) throws IOException {
    // 输入为空直接返回null
    if (bytes == null) {
      return null;
    }
    // 将字节数组转换回Long类型
    return Bytes.toLong(bytes);
  }

  /**
   * 比较两个Number，转换为Long后比较，null视为0。用于排序。
   *
   * @param num1 第一个待比较数值
   * @param num2 第二个待比较数值
   * @return -1 num1<num2，0 相等，1 num1>num2
   */
  @Override
  public int compare(Number num1, Number num2) {
    return Long.compare((num1 == null) ? 0L : num1.longValue(),
        (num2 == null) ? 0L : num2.longValue());
  }

  @Override
  public Number add(Number num1, Number num2, Number...numbers) {
    // 计算前两个数的和，null视为0
    long sum = ((num1 == null) ? 0L : num1.longValue()) +
        ((num2 == null) ? 0L : num2.longValue());
    // 遍历累加剩余所有参数
    for (Number num : numbers) {
      sum = sum + ((num == null) ? 0L : num.longValue());
    }
    return sum;
  }

  /**
   * 反转时间戳，使最新时间在HBase扫描时排在最前面。
   * 用于降序排序场景，配合HBase顺序扫描实现按时间倒序查询。
   *
   * @param key 原始时间戳长整型值
   * @return 反转后的长整型值
   */
  public static long invertLong(long key) {
    return Long.MAX_VALUE - key;
  }
}