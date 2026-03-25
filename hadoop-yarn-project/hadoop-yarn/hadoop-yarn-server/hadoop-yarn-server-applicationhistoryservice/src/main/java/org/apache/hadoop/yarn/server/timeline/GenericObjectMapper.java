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
package org.apache.hadoop.yarn.server.timeline;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 时间线数据存储的序列化工具类，提供对象JSON序列化和倒序长整型编码功能
 * 被LeveldbTimelineStore用于存储任意JSON数据，以及对实体按开始时间降序排序
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GenericObjectMapper {
  private static final byte[] EMPTY_BYTES = new byte[0];

  public static final ObjectReader OBJECT_READER;
  public static final ObjectWriter OBJECT_WRITER;

  // 静态初始化Jackson JSON读写实例
  static {
    ObjectMapper mapper = new ObjectMapper();
    OBJECT_READER = mapper.reader(Object.class);
    OBJECT_WRITER = mapper.writer();
  }

  /**
   * 将对象序列化为JSON字节数组，可配合read方法反序列化还原
   * 用于LeveldbTimelineStore存储任意JSON兼容的对象，无需预先指定类型
   *
   * @param o 待序列化对象
   * @return 对象的JSON字节数组表示，空对象返回空数组
   * @throws IOException 序列化写入失败时抛出
   */
  public static byte[] write(Object o) throws IOException {
    if (o == null) {
      return EMPTY_BYTES;
    }
    return OBJECT_WRITER.writeValueAsBytes(o);
  }

  /**
   * 从write方法生成的字节数组反序列化还原对象
   *
   * @param b 序列化得到的字节数组
   * @return 反序列化后的对象，空输入返回null
   * @throws IOException 反序列化读取失败时抛出
   */
  public static Object read(byte[] b) throws IOException {
    return read(b, 0);
  }

  /**
   * 从字节数组指定偏移位置反序列化还原对象
   *
   * @param b 序列化得到的字节数组
   * @param offset 反序列化起始偏移量
   * @return 反序列化后的对象，空输入返回null
   * @throws IOException 反序列化读取失败时抛出
   */
  public static Object read(byte[] b, int offset) throws IOException {
    if (b == null || b.length == 0) {
      return null;
    }
    return OBJECT_READER.readValue(b, offset, b.length - offset);
  }

  /**
   * 将长整型编码为8字节数组，使得编码后的字节数组按字典序排序时，原始长整型按降序排列
   * 用于Leveldb中按开始时间降序排序时间线实体
   *
   * @param l 待编码的长整型
   * @return 编码后的8字节数组
   */
  public static byte[] writeReverseOrderedLong(long l) {
    byte[] b = new byte[8];
    return writeReverseOrderedLong(l, b, 0);
  }

  /**
   * 将长整型编码到指定字节数组的指定偏移位置，保持倒序排序特性
   * 
   * @param l 待编码的长整型
   * @param b 目标字节数组
   * @param offset 写入起始偏移量
   * @return 编码后的字节数组
   */
  public static byte[] writeReverseOrderedLong(long l, byte[] b, int offset) {
    // 对最高位取反，实现符号翻转
    b[offset] = (byte)(0x7f ^ ((l >> 56) & 0xff));
    // 依次对中间字节按位取反
    for (int i = offset+1; i < offset+7; i++) {
      b[i] = (byte)(0xff ^ ((l >> 8*(7-i)) & 0xff));
    }
    // 对最低位按位取反
    b[offset+7] = (byte)(0xff ^ (l & 0xff));
    return b;
  }

  /**
   * 从指定偏移位置读取8字节，还原由writeReverseOrderedLong编码的倒序长整型
   *
   * @param b 编码后的字节数组
   * @param offset 读取起始偏移量
   * @return 还原后的原始长整型
   */
  public static long readReverseOrderedLong(byte[] b, int offset) {
    // 读取第一个字节到结果
    long l = b[offset] & 0xff;
    // 依次读取后续字节，拼接成长整型
    for (int i = 1; i < 8; i++) {
      l = l << 8;
      l = l | (b[offset+i]&0xff);
    }
    // 按位取反还原原始值
    return l ^ 0x7fffffffffffffffl;
  }

}