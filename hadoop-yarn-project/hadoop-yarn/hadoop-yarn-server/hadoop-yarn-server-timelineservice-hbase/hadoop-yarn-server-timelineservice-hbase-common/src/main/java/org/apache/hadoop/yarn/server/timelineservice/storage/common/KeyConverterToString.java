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
 * HBase时间线存储中，行键或列限定符的字符串编解码接口，需要具体类型实现编解码逻辑。
 * 用于在HBase存储层将自定义键和字符串表示形式互转，支持调试和日志场景。
 *
 * @param <T> 需要编解码的键类型
 */
public interface KeyConverterToString<T> {
  /**
   * 将指定类型的键编码为字符串。
   * @param key 需要编码的T类型键
   * @return 编码后的字符串表示
   */
  String encodeAsString(T key);

  /**
   * 将字符串形式的键解码为指定类型的键对象。
   * @param encodedKey 编码后的行键字符串
   * @return 解码后得到的T类型键对象
   */
  T decodeFromString(String encodedKey);
}