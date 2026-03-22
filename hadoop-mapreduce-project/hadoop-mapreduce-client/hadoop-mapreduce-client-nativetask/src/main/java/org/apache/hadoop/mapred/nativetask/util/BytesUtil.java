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

package org.apache.hadoop.mapred.nativetask.util;

import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Longs;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 字节数组工具类，为Native Task模块提供基础的字节数组与基础类型相互转换、字节数组可视化格式化能力
 * 用于本地任务处理过程中的序列化反序列化和调试输出
 */
@InterfaceAudience.Private
public class BytesUtil {

  private static final char[] HEX_CHARS =
      "0123456789abcdef".toCharArray();

  /**
   * 将大端字节数组从指定偏移位置转换为long类型值
   * @param bytes 输入字节数组
   * @param offset 起始偏移位置
   * @return 转换后的long值
   */
  public static long toLong(byte[] bytes, int offset) {
    return Longs.fromBytes(bytes[offset],
      bytes[offset + 1],
      bytes[offset + 2],
      bytes[offset + 3],
      bytes[offset + 4],
      bytes[offset + 5],
      bytes[offset + 6],
      bytes[offset + 7]);
  }

  /**
   * 将大端字节数组从指定偏移位置转换为int类型值
   * @param bytes 输入字节数组
   * @param offset 起始偏移位置
   * @return 转换后的int值
   */
  public static int toInt(byte[] bytes, int offset) {
    return Ints.fromBytes(bytes[offset],
      bytes[offset + 1],
      bytes[offset + 2],
      bytes[offset + 3]);
  }

  /**
   * 将字节数组按IEEE 754单精度浮点数格式转换为float类型值
   * @param bytes 输入字节数组
   * @return 转换后的float值
   */
  public static float toFloat(byte [] bytes) {
    return toFloat(bytes, 0);
  }

  /**
   * 将字节数组从指定偏移位置按IEEE 754单精度浮点数格式转换为float类型值
   * @param bytes 输入字节数组
   * @param offset 起始偏移位置
   * @return 转换后的float值
   */
  public static float toFloat(byte [] bytes, int offset) {
    return Float.intBitsToFloat(toInt(bytes, offset));
  }

  /**
   * 将整个字节数组按IEEE 754双精度浮点数格式转换为double类型值
   * @param bytes 输入字节数组
   * @return 转换后的double值
   */
  public static double toDouble(final byte [] bytes) {
    return toDouble(bytes, 0);
  }

  /**
   * 将字节数组从指定偏移位置按IEEE 754双精度浮点数格式转换为double类型值
   * @param bytes 输入字节数组
   * @param offset 起始偏移位置
   * @return 转换后的double值
   */
  public static double toDouble(final byte [] bytes, final int offset) {
    return Double.longBitsToDouble(toLong(bytes, offset));
  }

  /**
   * 将整个字节数组转换为可打印字符串，不可打印字符转为十六进制转义格式
   * @param b 输入字节数组
   * @return 可打印的字符串表示
   */
  public static String toStringBinary(final byte [] b) {
    if (b == null)
      return "null";
    return toStringBinary(b, 0, b.length);
  }

  /**
   * 将字节数组指定范围转换为可打印字符串，不可打印字符转为\\xHH格式的十六进制转义
   * @param b 输入字节数组
   * @param off 起始偏移位置
   * @param len 转换长度
   * @return 可打印的字符串表示
   */
  public static String toStringBinary(final byte [] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    // 处理偏移超出数组长度的边界情况
    if (off >= b.length) return result.toString();
    // 处理长度超出数组剩余部分的边界情况，截断长度
    if (off + len > b.length) len = b.length - off;
    // 遍历指定范围的每个字节
    for (int i = off; i < off + len ; ++i ) {
      // 获取无符号字节值
      int ch = b[i] & 0xFF;
      // 判断是否为可打印的安全字符：数字、大小写字母、常见符号
      if ( (ch >= '0' && ch <= '9')
        || (ch >= 'A' && ch <= 'Z')
        || (ch >= 'a' && ch <= 'z')
        || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0 ) {
        // 可打印字符直接追加
        result.append((char)ch);
      } else {
        // 不可打印字符转为十六进制转义格式
        result.append("\\x");
        result.append(HEX_CHARS[(ch >> 4) & 0x0F]);
        result.append(HEX_CHARS[ch & 0x0F]);
      }
    }
    return result.toString();
  }

  /**
   * 将布尔值转换为字节数组：true转为-1，false转为0
   * @param b 输入布尔值
   * @return 编码后的字节数组
   */
  public static byte [] toBytes(final boolean b) {
    return new byte[] { b ? (byte) -1 : (byte) 0 };
  }

  /**
   * 将float值转换为IEEE 754格式的字节数组
   * @param f 输入float值
   * @return 编码后的4字节字节数组
   */
  public static byte [] toBytes(final float f) {
    // Encode it as int
    return Ints.toByteArray(Float.floatToRawIntBits(f));
  }

  /**
   * 将double值转换为IEEE 754格式的字节数组
   * @param d 输入double值
   * @return 编码后的8字节字节数组
   */
  public static byte [] toBytes(final double d) {
    // Encode it as a long
    return Longs.toByteArray(Double.doubleToRawLongBits(d));
  }

}