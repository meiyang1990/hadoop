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
 * @file BufferStream.h
 * @brief 原生MapReduce任务基于内存缓冲区的输入输出流实现，提供基于内存块和字符串的流式读写能力
 */

#ifndef BUFFERSTREAM_H_
#define BUFFERSTREAM_H_

#include <string>
#include "lib/Streams.h"

namespace NativeTask {

using std::string;

/**
 * @class InputBuffer
 * @brief 基于固定内存缓冲区的输入流，继承自InputStream，提供从内存块读取数据的流式接口
 * 
 * 用于对已有内存缓冲区进行流式读取，支持随机定位、重置等操作，常用于反序列化场景
 */
class InputBuffer : public InputStream {
protected:
  const char * _buff;
  uint32_t _position;
  uint32_t _capacity;
public:
  InputBuffer()
      : _buff(NULL), _position(0), _capacity(0) {
  }

  InputBuffer(const char * buff, uint32_t capacity)
      : _buff(buff), _position(0), _capacity(capacity) {
  }

  InputBuffer(const string & src)
      : _buff(src.data()), _position(0), _capacity(src.length()) {
  }

  virtual ~InputBuffer() {
  }

  /**
   * @brief 移动当前读取位置到指定偏移
   * @param position 目标偏移量
   */
  virtual void seek(uint64_t position) {
    if (position <= _capacity) {
      _position = position;
    } else {
      _position = _capacity;
    }
  }

  /**
   * @brief 获取当前读取位置偏移
   * @return 当前偏移量
   */
  virtual uint64_t tell() {
    return _position;
  }

  /**
   * @brief 从缓冲区读取指定长度数据到目标内存
   * @param buff 目标内存缓冲区
   * @param length 需要读取的字节数
   * @return 实际读取的字节数，小于0表示读取结束
   */
  virtual int32_t read(void * buff, uint32_t length);

  /**
   * @brief 重置输入缓冲区，绑定到新的内存块
   * @param buff 新内存块地址
   * @param capacity 新内存块容量
   */
  void reset(const char * buff, uint32_t capacity) {
    _buff = buff;
    _position = 0;
    _capacity = capacity;
  }

  /**
   * @brief 重置输入缓冲区，绑定到字符串
   * @param src 源字符串
   */
  void reset(const string & src) {
    _buff = src.data();
    _position = 0;
    _capacity = src.length();
  }

  /**
   * @brief 倒回读取位置到缓冲区起始处
   */
  void rewind() {
    _position = 0;
  }
};

/**
 * @class OutputBuffer
 * @brief 基于固定内存缓冲区的输出流，继承自OutputStream，提供向内存块写入数据的流式接口
 * 
 * 用于向已有内存缓冲区进行流式写入，常用于序列化场景
 */
class OutputBuffer : public OutputStream {
protected:
  char * _buff;
  uint32_t _position;
  uint32_t _capacity;
public:
  OutputBuffer()
      : _buff(NULL), _position(0), _capacity(0) {
  }

  OutputBuffer(char * buff, uint32_t capacity)
      : _buff(buff), _position(0), _capacity(capacity) {
  }

  virtual ~OutputBuffer() {
  }

  /**
   * @brief 获取当前写入位置偏移
   * @return 当前偏移量
   */
  virtual uint64_t tell() {
    return _position;
  }

  /**
   * @brief 将指定长度数据写入缓冲区
   * @param buff 源数据地址
   * @param length 需要写入的字节数
   */
  virtual void write(const void * buff, uint32_t length);

  /**
   * @brief 清空缓冲区，重置写入位置
   */
  void clear() {
    _position = 0;
  }

  /**
   * @brief 重置输出缓冲区，绑定到新的内存块
   * @param buff 新内存块地址
   * @param capacity 新内存块容量
   */
  void reset(char * buff, uint32_t capacity) {
    _buff = buff;
    _position = 0;
    _capacity = capacity;
  }

  /**
   * @brief 获取当前已写入内容的字符串形式
   * @return 已写入数据构造的字符串
   */
  string getString() {
    return string(_buff, _position);
  }
};

/**
 * @class OutputStringStream
 * @brief 基于std::string的输出流，继承自OutputStream，提供动态向字符串写入数据的流式接口
 * 
 * 利用std::string的动态扩容能力，可写入任意长度数据，适合动态构造输出内容
 */
class OutputStringStream : public OutputStream {
protected:
  string * _dest;
public:
  OutputStringStream()
      : _dest(NULL) {
  }

  OutputStringStream(string & dest)
      : _dest(&dest) {
  }
  virtual ~OutputStringStream() {
  }

  /**
   * @brief 获取当前写入位置偏移
   * @return 当前偏移量，等于目标字符串长度
   */
  virtual uint64_t tell() {
    return _dest->length();
  }

  /**
   * @brief 将指定长度数据追加写入目标字符串
   * @param buff 源数据地址
   * @param length 需要写入的字节数
   */
  virtual void write(const void * buff, uint32_t length) {
    _dest->append((const char *)buff, length);
  }

  /**
   * @brief 重置输出流，绑定到新的目标字符串
   * @param dest 目标字符串指针
   */
  void reset(string * dest) {
    _dest = dest;
  }

  /**
   * @brief 清空目标字符串
   */
  void clear() {
    _dest->clear();
  }

  /**
   * @brief 获取当前写入的完整字符串
   * @return 目标字符串的拷贝
   */
  string getString() {
    return *_dest;
  }
};

} // namespace NativeTask

#endif /* BUFFERSTREAM_H_ */