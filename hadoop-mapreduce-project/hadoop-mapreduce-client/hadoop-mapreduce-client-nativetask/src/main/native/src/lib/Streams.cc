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
 * @file Streams.cc
 * @brief 原生任务流处理实现，提供输入输出流抽象及带校验和的流包装类
 * 
 * 实现了Hadoop MapReduce原生任务中基础的IO流抽象接口，支持带校验和验证的输入输出流装饰器，
 * 用于原生任务在处理数据时保证数据完整性，适配本地MapReduce任务的底层IO需求。
 */

#include "lib/Streams.h"
#include "lib/commons.h"
#include "util/Checksum.h"

namespace NativeTask {

/////////////////////////////////////////////////////////////

/**
 * @brief 输入流定位操作，默认不支持
 * @param position 目标定位偏移量
 */
void InputStream::seek(uint64_t position) {
  THROW_EXCEPTION(UnsupportException, "seek not support");
}

/**
 * @brief 获取当前流位置，默认不支持
 * @return 当前流偏移量
 */
uint64_t InputStream::tell() {
  THROW_EXCEPTION(UnsupportException, "tell not support");
}

/**
 * @brief 读取指定长度的数据，阻塞直到读满或到达流末尾
 * @param buff 存储读取数据的缓冲区
 * @param length 需要读取的字节长度
 * @return 实际读取的字节数，流起始就读到末尾则返回-1
 */
int32_t InputStream::readFully(void * buff, uint32_t length) {
  int32_t ret = 0;
  while (length > 0) {
    // 循环读取直到满足长度要求或流结束
    int32_t rd = read(buff, length);
    if (rd <= 0) {
      return ret > 0 ? ret : -1;
    }
    ret += rd;
    // 移动缓冲区指针
    buff = ((char *)buff) + rd;
    length -= rd;
  }
  return ret;
}

/**
 * @brief 将当前输入流所有数据写出到目标输出流
 * @param out 目标输出流
 * @param bufferHint 读写缓冲区大小
 */
void InputStream::readAllTo(OutputStream & out, uint32_t bufferHint) {
  // 分配指定大小的缓冲区
  char * buffer = new char[bufferHint];
  while (true) {
    int32_t rd = read(buffer, bufferHint);
    if (rd <= 0) {
      break;
    }
    // 将读到的数据写出到目标输出流
    out.write(buffer, rd);
  }
  delete buffer;
}

/////////////////////////////////////////////////////////////

/**
 * @brief 获取输出流当前位置，默认不支持
 * @return 当前输出流偏移量
 */
uint64_t OutputStream::tell() {
  THROW_EXCEPTION(UnsupportException, "tell not support");
}

///////////////////////////////////////////////////////////

/**
 * @brief 构造带校验和的输入流
 * @param stream 被装饰的底层输入流
 * @param type 校验和类型
 */
ChecksumInputStream::ChecksumInputStream(InputStream * stream, ChecksumType type)
    : FilterInputStream(stream), _type(type), _limit(-1) {
  resetChecksum();
}

/**
 * @brief 重置校验和状态，重新计算
 */
void ChecksumInputStream::resetChecksum() {
  _checksum = Checksum::init(_type);
}

/**
 * @brief 获取当前计算得到的校验和值
 * @return 计算完成的校验和结果
 */
uint32_t ChecksumInputStream::getChecksum() {
  return Checksum::getValue(_type, _checksum);
}

/**
 * @brief 从底层流读取数据并更新校验和
 * @param buff 存储读取数据的缓冲区
 * @param length 需要读取的字节长度
 * @return 实际读取的字节数，流末尾返回-1
 */
int32_t ChecksumInputStream::read(void * buff, uint32_t length) {
  // 无读取长度限制，读取所有请求长度
  if (_limit < 0) {
    int32_t ret = _stream->read(buff, length);
    if (ret > 0) {
      Checksum::update(_type, _checksum, buff, ret);
    }
    return ret;
  } else if (_limit == 0) {
    // 已达到长度限制，返回流结束
    return -1;
  } else {
    // 按剩余限制长度读取
    int64_t rd = _limit < length ? _limit : length;
    int32_t ret = _stream->read(buff, rd);
    if (ret > 0) {
      _limit -= ret;
      Checksum::update(_type, _checksum, buff, ret);
    }
    return ret;
  }
}

///////////////////////////////////////////////////////////

/**
 * @brief 构造带校验和的输出流
 * @param stream 被装饰的底层输出流
 * @param type 校验和类型
 */
ChecksumOutputStream::ChecksumOutputStream(OutputStream * stream, ChecksumType type)
    : FilterOutputStream(stream), _type(type) {
  resetChecksum();
}

/**
 * @brief 重置校验和状态，重新计算
 */
void ChecksumOutputStream::resetChecksum() {
  _checksum = Checksum::init(_type);
}

/**
 * @brief 获取当前计算得到的校验和值
 * @return 计算完成的校验和结果
 */
uint32_t ChecksumOutputStream::getChecksum() {
  return Checksum::getValue(_type, _checksum);
}

/**
 * @brief 写出数据并更新校验和
 * @param buff 待写出数据缓冲区
 * @param length 待写出数据字节长度
 */
void ChecksumOutputStream::write(const void * buff, uint32_t length) {
  Checksum::update(_type, _checksum, buff, length);
  _stream->write(buff, length);
}

} // namespace NativeTask