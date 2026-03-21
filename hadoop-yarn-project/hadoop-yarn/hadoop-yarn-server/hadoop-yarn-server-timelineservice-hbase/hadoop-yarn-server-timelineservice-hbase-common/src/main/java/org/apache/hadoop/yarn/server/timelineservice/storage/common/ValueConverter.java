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

/**
 * YARN时间线服务HBase存储的数值编解码接口，用于对列前缀或列关联的数值进行编码/解码。
 * 不同类型的数据可实现该接口自定义存储序列化格式。
 */
public interface ValueConverter {

  /**
   * 将对象编码为存储用的字节数组。
   *
   * @param value 待编码的对象值
   * @return 编码后的字节数组
   * @throws IOException 编码过程出错时抛出异常
   */
  byte[] encodeValue(Object value) throws IOException;

  /**
   * 将字节数组解码还原为对象。
   *
   * @param bytes 待解码的字节数组
   * @return 解码后的对象
   * @throws IOException 解码过程出错时抛出异常
   */
  Object decodeValue(byte[] bytes) throws IOException;
}