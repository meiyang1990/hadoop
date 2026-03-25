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
 * HBase时间线存储键转换器接口，定义行键和列的编解码规范，
 * 用于将自定义类型数据转换为HBase存储所需的字节数组，反之亦然。
 *
 * @param <T> 需要编解码的键类型
 */
public interface KeyConverter<T> {
  /**
   * 将指定键编码为字节数组，用于存储到HBase。
   *
   * @param key 待编码的键对象
   * @return 编码后的字节数组
   */
  byte[] encode(T key);

  /**
   * 将字节数组解码为指定类型的键对象。
   *
   * @param bytes 待解码的HBase字节数据
   * @return 解码后的键对象
   */
  T decode(byte[] bytes);
}