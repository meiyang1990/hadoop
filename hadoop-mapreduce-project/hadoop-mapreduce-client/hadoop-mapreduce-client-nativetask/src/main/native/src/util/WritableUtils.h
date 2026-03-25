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

/**
 * @file WritableUtils.h
 * @brief Hadoop Writable序列化格式工具类头文件，提供变长整数和常见类型的读写能力
 * 
 * 该文件属于Hadoop MapReduce原生任务模块，提供兼容Java Hadoop Writable格式
 * 的序列化/反序列化工具，用于原生任务处理Hadoop数据流。
 */

#ifndef WRITABLEUTILS_H_
#define WRITABLEUTILS_H_

#include <stdint.h>
#include <string>
#include "lib/Streams.h"
#include "NativeTask.h"

namespace NativeTask {

/**
 * @brief 根据Java类名转换为原生MapReduce键值类型枚举
 * @param clazz Java类全限定名
 * @return 对应原生键值类型枚举值
 */
KeyValueType JavaClassToKeyValueType(const std::string & clazz);

using std::string;

/**
 * @class WritableUtils
 * @brief Writable格式序列化工具类，兼容Hadoop Java端的Writable序列化规则
 * 
 * 核心功能包括：
 * 1. 支持Hadoop自定义变长整数VLong/VInt的编码解码
 * 2. 提供基于内存缓冲区和流接口两种读写方式
 * 3. 支持常见基本类型和文本类型的读写
 */
class WritableUtils {
protected:
  /**
   * @brief 内部方法：读取大偏移量VLong，处理单字节无法容纳的情况
   * @param pos 输入缓冲区起始地址
   * @param len [out] 实际读取的字节长度
   * @return 解码后的VLong值
   */
  static int64_t ReadVLongInner(const char * pos, uint32_t & len);

  /**
   * @brief 内部方法：写入大数值VLong，处理单字节无法容纳的情况
   * @param value 待写入的VLong值
   * @param pos 输出缓冲区起始地址
   * @param len [out] 实际写入的字节长度
   */
  static void WriteVLongInner(int64_t value, char * pos, uint32_t & len);

  /**
   * @brief 内部方法：计算大数值VLong编码后的字节长度
   * @param value 待编码的VLong值
   * @return 编码后需要的字节长度
   */
  static uint32_t GetVLongSizeInner(int64_t value);
public:
  /**
   * @brief 根据VLong首字节解码出整个VLong的字节长度
   * @param ch VLong首字节
   * @return VLong总字节长度
   */
  inline static uint32_t DecodeVLongSize(int8_t ch) {
    if (ch >= -112) {
      return 1;
    } else if (ch < -120) {
      return -119 - ch;
    }
    return -111 - ch;
  }

  /**
   * @brief 根据缓冲区首地址解码出VLong的字节长度
   * @param pos VLong在缓冲区中的起始地址
   * @return VLong总字节长度
   */
  inline static uint32_t DecodeVLongSize(const char * pos) {
    return DecodeVLongSize(*pos);
  }

  /**
   * @brief 计算VLong编码后的字节长度
   * @param value 待编码的64位整数
   * @return 编码后需要的字节长度
   */
  inline static uint32_t GetVLongSize(int64_t value) {
    if (value >= -112 && value <= 127) {
      return 1;
    }
    return GetVLongSizeInner(value);
  }

  /**
   * @brief 从内存缓冲区读取VLong
   * @param pos VLong在缓冲区中的起始地址
   * @param len [out] 实际读取的字节长度
   * @return 解码后的VLong值
   */
  inline static int64_t ReadVLong(const char * pos, uint32_t & len) {
    if (*pos >= (char)-112) {
      len = 1;
      return *pos;
    } else {
      return ReadVLongInner(pos, len);
    }
  }

  /**
   * @brief 从内存缓冲区读取VInt（变长32位整数）
   * @param pos VInt在缓冲区中的起始地址
   * @param len [out] 实际读取的字节长度
   * @return 解码后的VInt值
   */
  inline static int32_t ReadVInt(const char * pos, uint32_t & len) {
    return (int32_t)ReadVLong(pos, len);
  }

  /**
   * @brief 将VLong写入内存缓冲区
   * @param v 待写入的64位整数
   * @param target 输出缓冲区起始地址
   * @param written [out] 实际写入的字节长度
   */
  inline static void WriteVLong(int64_t v, char * target, uint32_t & written) {
    if (v <= 127 && v >= -112) {
      written = 1;
      *target = (char)v;
    } else {
      WriteVLongInner(v, target, written);
    }
  }

  /**
   * @brief 将VInt写入内存缓冲区
   * @param v 待写入的32位整数
   * @param target 输出缓冲区起始地址
   * @param written [out] 实际写入的字节长度
   */
  inline static void WriteVInt(int32_t v, char * target, uint32_t & written) {
    WriteVLong(v, target, written);
  }

  // 流读写接口

  /**
   * @brief 从输入流读取VLong
   * @param stream 输入流对象
   * @return 解码后的VLong值
   */
  static int64_t ReadVLong(InputStream * stream);

  /**
   * @brief 从输入流读取固定长度64位整数
   * @param stream 输入流对象
   * @return 解码后的long值
   */
  static int64_t ReadLong(InputStream * stream);

  /**
   * @brief 从输入流读取固定长度32位整数
   * @param stream 输入流对象
   * @return 解码后的int值
   */
  static int32_t ReadInt(InputStream * stream);

  /**
   * @brief 从输入流读取固定长度16位整数
   * @param stream 输入流对象
   * @return 解码后的short值
   */
  static int16_t ReadShort(InputStream * stream);

  /**
   * @brief 从输入流读取单精度浮点数
   * @param stream 输入流对象
   * @return 解码后的float值
   */
  static float ReadFloat(InputStream * stream);

  /**
   * @brief 从输入流读取Hadoop Text类型
   * @param stream 输入流对象
   * @return 解码后的字符串
   */
  static string ReadText(InputStream * stream);

  /**
   * @brief 从输入流读取字节数组类型
   * @param stream 输入流对象
   * @return 解码后的字节数组（存为string）
   */
  static string ReadBytes(InputStream * stream);

  /**
   * @brief 从输入流读取UTF8编码字符串
   * @param stream 输入流对象
   * @return 解码后的字符串
   */
  static string ReadUTF8(InputStream * stream);

  /**
   * @brief 将VLong写入输出流
   * @param stream 输出流对象
   * @param v 待写入的VLong值
   */
  static void WriteVLong(OutputStream * stream, int64_t v);

  /**
   * @brief 将固定长度64位整数写入输出流
   * @param stream 输出流对象
   * @param v 待写入的long值
   */
  static void WriteLong(OutputStream * stream, int64_t v);

  /**
   * @brief 将固定长度32位整数写入输出流
   * @param stream 输出流对象
   * @param v 待写入的int值
   */
  static void WriteInt(OutputStream * stream, int32_t v);

  /**
   * @brief 将固定长度16位整数写入输出流
   * @param stream 输出流对象
   * @param v 待写入的short值
   */
  static void WriteShort(OutputStream * stream, int16_t v);

  /**
   * @brief 将单精度浮点数写入输出流
   * @param stream 输出流对象
   * @param v 待写入的float值
   */
  static void WriteFloat(OutputStream * stream, float v);

  /**
   * @brief 将字符串以Hadoop Text格式写入输出流
   * @param stream 输出流对象
   * @param v 待写入的字符串
   */
  static void WriteText(OutputStream * stream, const string & v);

  /**
   * @brief 将字节数组写入输出流
   * @param stream 输出流对象
   * @param v 待写入的字节数组（存为string）
   */
  static void WriteBytes(OutputStream * stream, const string & v);

  /**
   * @brief 将字符串以UTF8编码写入输出流
   * @param stream 输出流对象
   * @param v 待写入的字符串
   */
  static void WriteUTF8(OutputStream * stream, const string & v);

  /**
   * @brief 将二进制Writable数据转换为可打印字符串，用于调试和日志
   * @param dest 输出目标字符串
   * @param type 键值数据类型
   * @param data 二进制数据起始地址
   * @param length 二进制数据长度
   */
  static void toString(string & dest, KeyValueType type, const void * data, uint32_t length);
};

} // namespace NativeTask

#endif /* WRITABLEUTILS_H_ */