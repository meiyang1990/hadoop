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

import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;

/**
 * YARN时间线服务HBase存储的通用值转换器，基于GenericObjectMapper实现对象与字节数组的互转，
 * 用于将时间线数据对象序列化为字节存储到HBase，以及将HBase中存储的字节反序列化为对象。
 */
public final class GenericConverter implements ValueConverter {
  // 单例实例
  private static final GenericConverter INSTANCE = new GenericConverter();

  private GenericConverter() {
  }

  /**
   * 获取GenericConverter单例实例。
   * @return GenericConverter单例
   */
  public static GenericConverter getInstance() {
    return INSTANCE;
  }

  @Override
  public byte[] encodeValue(Object value) throws IOException {
    return GenericObjectMapper.write(value);
  }

  @Override
  public Object decodeValue(byte[] bytes) throws IOException {
    return GenericObjectMapper.read(bytes);
  }
}