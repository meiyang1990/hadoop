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
 * @file WritableUtils.cc
 * @brief Hadoop Writable类型工具实现，提供MapReduce本地任务中Hadoop Writable类型的序列化/反序列化能力
 *
 * 负责将Java端Hadoop Writable类型转换为本地C++端可处理的类型，
 * 实现变长整数VLong、各种基础类型以及Text类型的读写操作
 */

#include "lib/commons.h"
#include "util/StringUtil.h"
#include "util/WritableUtils.h"

namespace NativeTask {

/**
 * @brief 根据Java类名转换为本地键值对枚举类型
 * @param clazz Java全限定类名字符串
 * @return 对应当前类型的KeyValueType枚举，找不到返回UnknownType
 */
KeyValueType JavaClassToKeyValueType(const std::string & clazz) {
  if (clazz == "org.apache.hadoop.io.Text") {
    return TextType;
  }
  if (clazz == "org.apache.hadoop.io.BytesWritable") {
    return BytesType;
  }
  if (clazz == "org.apache.hadoop.io.ByteWritable") {
    return ByteType;
  }
  if (clazz == "org.apache.hadoop.io.BooleanWritable") {
    return BoolType;
  }
  if (clazz == "org.apache.hadoop.io.IntWritable") {
    return IntType;
  }
  if (clazz == "org.apache.hadoop.io.LongWritable") {
    return LongType;
  }
  if (clazz == "org.apache.hadoop.io.FloatWritable") {
    return FloatType;
  }
  if (clazz == "org.apache.hadoop.io.DoubleWritable") {
    return DoubleType;
  }
  if (clazz == "org.apache.hadoop.io.MD5Hash") {
    return MD5HashType;
  }
  if (clazz == "org.apache.hadoop.io.VIntWritable") {
    return VIntType;
  }
  if (clazz == "org.apache.hadoop.io.VLongWritable") {
    return VLongType;
  }
  return UnknownType;
}

/**
 * @brief 从内存缓冲区解析VLong变长整数内部实现
 * @param pos 缓冲区起始位置指针
 * @param len 输出参数，存储解析得到的VLong总字节长度
 * @return 解析得到的int64_t整数值
 */
int64_t WritableUtils::ReadVLongInner(const char * pos, uint32_t & len) {
  bool neg = *pos < -120;
  len = neg ? (-119 - *pos) : (-111 - *pos);
  const char * end = pos + len;
  int64_t value = 0;
  while (++pos < end) {
    value = (value << 8) | *(uint8_t*)pos;
  }
  return neg ? (value ^ -1LL) : value;
}

/**
 * @brief 计算VLong变长整数序列化后的字节长度内部实现
 * @param value 要计算的64位整数值
 * @return VLong序列化后需要的字节数
 */
uint32_t WritableUtils::GetVLongSizeInner(int64_t value) {
  if (value < 0) {
    value ^= -1L; // take one's complement'
  }

  if (value < (1LL << 8)) {
    return 2;
  } else if (value < (1LL << 16)) {
    return 3;
  } else if (value < (1LL << 24)) {
    return 4;
  } else if (value < (1LL << 32)) {
    return 5;
  } else if (value < (1LL << 40)) {
    return 6;
  } else if (value < (1LL << 48)) {
    return 7;
  } else if (value < (1LL << 56)) {
    return 8;
  } else {
    return 9;
  }
}

/**
 * @brief 将64位整数序列化为VLong格式写入内存缓冲区内部实现
 * @param v 要序列化的64位整数值
 * @param pos 输出缓冲区起始位置指针
 * @param len 输出参数，存储序列化后的总字节长度
 */
void WritableUtils::WriteVLongInner(int64_t v, char * pos, uint32_t & len) {
  char base;
  if (v >= 0) {
    base = -113;
  } else {
    v ^= -1L; // take one's complement
    base = -121;
  }
  uint64_t value = v;
  if (value < (1 << 8)) {
    *(pos++) = base;
    *(uint8_t*)(pos) = value;
    len = 2;
  } else if (value < (1 << 16)) {
    *(pos++) = base - 1;
    *(uint8_t*)(pos++) = value >> 8;
    *(uint8_t*)(pos) = value;
    len = 3;
  } else if (value < (1 << 24)) {
    *(pos++) = base - 2;
    *(uint8_t*)(pos++) = value >> 16;
    *(uint8_t*)(pos++) = value >> 8;
    *(uint8_t*)(pos) = value;
    len = 4;
  } else if (value < (1ULL << 32)) {
    *(pos++) = base - 3;
    *(uint32_t*)(pos) = bswap((uint32_t)value);
    len = 5;
  } else if (value < (1ULL << 40)) {
    *(pos++) = base - 4;
    *(uint32_t*)(pos) = bswap((uint32_t)(value >> 8));
    *(uint8_t*)(pos + 4) = value;
    len = 6;
  } else if (value < (1ULL << 48)) {
    *(pos++) = base - 5;
    *(uint32_t*)(pos) = bswap((uint32_t)(value >> 16));
    *(uint8_t*)(pos + 4) = value >> 8;
    *(uint8_t*)(pos + 5) = value;
    len = 7;
  } else if (value < (1ULL << 56)) {
    *(pos++) = base - 6;
    *(uint32_t*)(pos) = bswap((uint32_t)(value >> 24));
    *(uint8_t*)(pos + 4) = value >> 16;
    *(uint8_t*)(pos + 5) = value >> 8;
    *(uint8_t*)(pos + 6) = value;
    len = 8;
  } else {
    *(pos++) = base - 7;
    *(uint64_t*)pos = bswap64(value);
    len = 9;
  }
}

/**
 * @brief 从输入流读取VLong变长整数
 * @param stream 输入流对象指针
 * @return 解析得到的64位整数值
 * @throws IOException 读取到流末尾抛出IO异常
 */
int64_t WritableUtils::ReadVLong(InputStream * stream) {
  char buff[10];
  if (stream->read(buff, 1) != 1) {
    THROW_EXCEPTION(IOException, "ReadVLong reach EOF");
  }
  uint32_t len = DecodeVLongSize(buff);
  if (len > 1) {
    len--;
    if (stream->readFully(buff + 1, len) != (int32_t)len) {
      THROW_EXCEPTION(IOException, "ReadVLong reach EOF");
    }
  }
  return ReadVLong(buff, len);
}

/**
 * @brief 从输入流读取64位长整数
 * @param stream 输入流对象指针
 * @return 解析得到的64位整数值
 * @throws IOException 读取到流末尾抛出IO异常
 */
int64_t WritableUtils::ReadLong(InputStream * stream) {
  int64_t ret;
  if (stream->readFully(&ret, 8) != 8) {
    THROW_EXCEPTION(IOException, "ReadLong reach EOF");
  }
  return (int64_t)bswap64(ret);
}

/**
 * @brief 从输入流读取32位整数
 * @param stream 输入流对象指针
 * @return 解析得到的32位整数值
 * @throws IOException 读取到流末尾抛出IO异常
 */
int32_t WritableUtils::ReadInt(InputStream * stream) {
  int32_t ret;
  if (stream->readFully(&ret, 4) != 4) {
    THROW_EXCEPTION(IOException, "ReadInt reach EOF");
  }
  return (int32_t)bswap(ret);
}

/**
 * @brief 从输入流读取16位短整数
 * @param stream 输入流对象指针
 * @return 解析得到的16位整数值
 * @throws IOException 读取到流末尾抛出IO异常
 */
int16_t WritableUtils::ReadShort(InputStream * stream) {
  uint16_t ret;
  if (stream->readFully(&ret, 2) != 2) {
    THROW_EXCEPTION(IOException, "ReadShort reach EOF");
  }
  return (int16_t)((ret >> 8) | (ret << 8));
}

/**
 * @brief 从输入流读取单精度浮点数
 * @param stream 输入流对象指针
 * @return 解析得到的float浮点数
 * @throws IOException 读取到流末尾抛出IO异常
 */
float WritableUtils::ReadFloat(InputStream * stream) {
  uint32_t ret;
  if (stream->readFully(&ret, 4) != 4) {
    THROW_EXCEPTION(IOException, "ReadFloat reach EOF");
  }
  ret = bswap(ret);
  return *(float*)&ret;
}

/**
 * @brief 从输入流读取Text类型
 * @param stream 输入流对象指针
 * @return 解析得到的字符串
 * @throws IOException 读取到流末尾抛出IO异常
 */
string WritableUtils::ReadText(InputStream * stream) {
  int64_t len = ReadVLong(stream);
  string ret = string(len, '\0');
  if (stream->readFully((void *)ret.data(), len) != len) {
    THROW_EXCEPTION_EX(IOException, "ReadString reach EOF, need %d", len);
  }
  return ret;
}

/**
 * @brief 从输入流读取BytesWritable类型
 * @param stream 输入流对象指针
 * @return 解析得到的二进制字符串
 * @throws IOException 读取到流末尾抛出IO异常
 */
string WritableUtils::ReadBytes(InputStream * stream) {
  int32_t len = ReadInt(stream);
  string ret = string(len, '\0');
  if (stream->readFully((void *)ret.data(), len) != len) {
    THROW_EXCEPTION_EX(IOException, "ReadString reach EOF, need %d", len);
  }
  return ret;
}

/**
 * @brief 从输入流读取UTF8编码字符串
 * @param stream 输入流对象指针
 * @return 解析得到的UTF8字符串
 * @throws IOException 读取到流末尾抛出IO异常
 */
string WritableUtils::ReadUTF8(InputStream * stream) {
  int16_t len = ReadShort(stream);
  string ret = string(len, '\0');
  if (stream->readFully((void *)ret.data(), len) != len) {
    THROW_EXCEPTION_EX(IOException, "ReadString reach EOF, need %d", len);
  }
  return ret;
}

/**
 * @brief 将VLong变长整数写入输出流
 * @param stream 输出流对象指针
 * @param v 要写入的64位整数值
 */
void WritableUtils::WriteVLong(OutputStream * stream, int64_t v) {
  char buff[10];
  uint32_t len;
  WriteVLong(v, buff, len);
  stream->write(buff, len);
}

/**
 * @brief 将64位长整数写入输出流
 * @param stream 输出流对象指针
 * @param v 要写入的64位整数值
 */
void WritableUtils::WriteLong(OutputStream * stream, int64_t v) {
  uint64_t be = bswap64((uint64_t)v);
  stream->write(&be, 8);
}

/**
 * @brief 将32位整数写入输出流
 * @param stream 输出流对象指针
 * @param v 要写入的32位整数值
 */
void WritableUtils::WriteInt(OutputStream * stream, int32_t v) {
  uint32_t be = bswap((uint32_t)v);
  stream->write(&be, 4);
}

/**
 * @brief 将16位短整数写入输出流
 * @param stream 输出流对象指针
 * @param v 要写入的16位整数值
 */
void WritableUtils::WriteShort(OutputStream * stream, int16_t v) {
  uint16_t be = v;
  be = ((be >> 8) | (be << 8));
  stream->write(&be, 2);
}

/**
 * @brief 将单精度浮点数写入输出流
 * @param stream 输出流对象指针
 * @param v 要写入的float浮点数
 */
void WritableUtils::WriteFloat(OutputStream * stream, float v) {
  uint32_t intv = *(uint32_t*)&v;
  intv = bswap(intv);
  stream->write(&intv, 4);
}

/**
 * @brief 将Text类型写入输出流
 * @param stream 输出流对象指针
 * @param v 要写入的字符串
 */
void WritableUtils::WriteText(OutputStream * stream, const string & v) {
  WriteVLong(stream, v.length());
  stream->write(v.c_str(), (uint32_t)v.length());
}

/**
 * @brief 将BytesWritable类型写入输出流
 * @param stream 输出流对象指针
 * @param v 要写入的二进制字符串
 */
void WritableUtils::WriteBytes(OutputStream * stream, const string & v) {
  WriteInt(stream, (int32_t)v.length());
  stream->write(v.c_str(), (uint32_t)v.length());
}

/**
 * @brief 将UTF8编码字符串写入输出流
 * @param stream 输出流对象指针
 * @param v 要写入的UTF8字符串
 * @throws IOException 字符串长度超过65535抛出异常
 */
void WritableUtils::WriteUTF8(OutputStream * stream, const string & v) {
  if (v.length() > 65535) {
    THROW_EXCEPTION_EX(IOException, "string too long (%lu) for WriteUTF8", v.length());
  }
  WriteShort(stream, (int16_t)v.length());
  stream->write(v.c_str(), (uint32_t)v.length());
}

/**
 * @brief 将不同类型的键值数据转换为字符串形式
 * @param dest 目标字符串，转换结果追加到此处
 * @param type 数据类型（KeyValueType枚举）
 * @param data 原始二进制数据指针
 * @param length 原始二进制数据长度
 */
void WritableUtils::toString(string & dest, KeyValueType type, const void * data, uint32_t length) {
  switch (type) {
  case TextType:
    dest.append((const char*)data, length);
    break;
  case BytesType:
    dest.append((const char*)data, length);
    break;
  case ByteType:
    dest.append(1, *(char*)data);
    break;
  case BoolType:
    dest.append(*(uint8_t*)data ? "true" : "false");
    break;
  case IntType:
    dest.append(StringUtil::ToString((int32_t)bswap(*(uint32_t*)data)));
    break;
  case LongType:
    dest.append(StringUtil::ToString((int64_t)bswap64(*(uint64_t*)data)));
    break;
  case FloatType:
    dest