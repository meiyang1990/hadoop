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
 * @file Streams.h
 * @brief Hadoop NativeTask 流抽象基类及装饰器实现
 * 
 * 本文件定义了NativeTask本地任务模块统一的输入输出流抽象接口，
 * 提供了带限制长度、校验和计算等装饰器实现，支撑本地任务的IO操作。
 */

#ifndef STREAMS_H_
#define STREAMS_H_

#include "util/Checksum.h"

namespace NativeTask {

class OutputStream;

/**
 * @class InputStream
 * @brief 输入流抽象基类，定义统一的输入流读取接口
 * 
 * 所有具体输入流实现都继承自该类，通过抽象接口隔离不同来源的数据读取，
 * 支持装饰器模式扩展额外功能（如长度限制、校验和计算）。
 */
class InputStream {
public:
  InputStream() {
  }

  virtual ~InputStream() {
  }

  /**
   * @brief 移动读取指针到指定位置
   * @param position 目标偏移位置（字节）
   */
  virtual void seek(uint64_t position);

  /**
   * @brief 获取当前读取指针位置
   * @return 当前偏移位置（字节）
   */
  virtual uint64_t tell();

  /**
   * @brief 从流中读取指定长度的数据到缓冲区
   * @param buff 目标缓冲区地址
   * @param length 期望读取长度（字节）
   * @return 实际读取的字节数，-1表示到达流末尾
   */
  virtual int32_t read(void * buff, uint32_t length) {
    return -1;
  }

  /**
   * @brief 关闭输入流，释放资源
   */
  virtual void close() {
  }

  /**
   * @brief 完全读取指定长度数据，直到读取到足够数据或流结束
   * @param buff 目标缓冲区地址
   * @param length 期望读取长度（字节）
   * @return 实际读取的字节数，-1表示流提前结束
   */
  virtual int32_t readFully(void * buff, uint32_t length);

  /**
   * @brief 将当前输入流的所有数据写入到输出流
   * @param out 目标输出流
   * @param bufferHint 传输缓冲区大小提示（字节）
   */
  void readAllTo(OutputStream & out, uint32_t bufferHint = 1024 * 4);
};

/**
 * @class OutputStream
 * @brief 输出流抽象基类，定义统一的输出流写入接口
 * 
 * 所有具体输出流实现都继承自该类，通过抽象接口隔离不同目的地的数据写入，
 * 支持装饰器模式扩展额外功能（如校验和计算）。
 */
class OutputStream {
public:
  OutputStream() {
  }

  virtual ~OutputStream() {
  }

  /**
   * @brief 获取当前写入指针位置
   * @return 当前偏移位置（字节）
   */
  virtual uint64_t tell();

  /**
   * @brief 将缓冲区数据写入到流
   * @param buff 源缓冲区地址
   * @param length 写入长度（字节）
   */
  virtual void write(const void * buff, uint32_t length) {
  }

  /**
   * @brief 刷新流缓冲区，将缓存数据写出到底层
   */
  virtual void flush() {
  }

  /**
   * @brief 关闭输出流，释放资源
   */
  virtual void close() {
  }
};

/**
 * @class FilterInputStream
 * @brief 输入流装饰器基类，为已有输入流扩展额外功能
 * 
 * 实现装饰器模式，默认透传所有接口调用到底层输入流，子类只需重写需要扩展的方法。
 */
class FilterInputStream : public InputStream {
protected:
  InputStream * _stream; // 被装饰的底层输入流
public:
  /**
   * @brief 构造函数，绑定被装饰的输入流
   * @param stream 被装饰的底层输入流
   */
  FilterInputStream(InputStream * stream)
      : _stream(stream) {
  }

  virtual ~FilterInputStream() {
  }

  /**
   * @brief 重新设置被装饰的输入流
   * @param stream 新的底层输入流
   */
  void setStream(InputStream * stream) {
    _stream = stream;
  }

  /**
   * @brief 获取当前被装饰的底层输入流
   * @return 底层输入流指针
   */
  InputStream * getStream() {
    return _stream;
  }

  virtual void seek(uint64_t position) {
    _stream->seek(position);
  }

  virtual uint64_t tell() {
    return _stream->tell();
  }

  virtual int32_t read(void * buff, uint32_t length) {
    return _stream->read(buff, length);
  }
};

/**
 * @class FilterOutputStream
 * @brief 输出流装饰器基类，为已有输出流扩展额外功能
 * 
 * 实现装饰器模式，默认透传所有接口调用到底层输出流，子类只需重写需要扩展的方法。
 */
class FilterOutputStream : public OutputStream {
protected:
  OutputStream * _stream; // 被装饰的底层输出流
public:
  /**
   * @brief 构造函数，绑定被装饰的输出流
   * @param stream 被装饰的底层输出流
   */
  FilterOutputStream(OutputStream * stream)
      : _stream(stream) {
  }

  virtual ~FilterOutputStream() {
  }

  /**
   * @brief 重新设置被装饰的输出流
   * @param stream 新的底层输出流
   */
  void setStream(OutputStream * stream) {
    _stream = stream;
  }

  /**
   * @brief 获取当前被装饰的底层输出流
   * @return 底层输出流指针
   */
  OutputStream * getStream() {
    return _stream;
  }

  virtual uint64_t tell() {
    return _stream->tell();
  }

  virtual void write(const void * buff, uint32_t length) {
    _stream->write(buff, length);
  }

  virtual void flush() {
    _stream->flush();
  }

  virtual void close() {
    flush();
  }
};

/**
 * @class LimitInputStream
 * @brief 限制读取长度的输入流装饰器
 * 
 * 确保最多只从底层输入流读取指定字节数的数据，超过限制后直接返回流结束，
 * 用于从大输入流中分割出固定长度的分片数据。
 */
class LimitInputStream : public FilterInputStream {
protected:
  int64_t _limit; // 剩余可读取字节数
public:
  /**
   * @brief 构造函数，绑定输入流并设置读取限制
   * @param stream 底层输入流
   * @param limit 最大可读取字节数，小于0表示不限制
   */
  LimitInputStream(InputStream * stream, int64_t limit)
      : FilterInputStream(stream), _limit(limit) {
  }

  virtual ~LimitInputStream() {
  }

  /**
   * @brief 获取当前剩余可读取字节数
   * @return 剩余字节数
   */
  int64_t getLimit() {
    return _limit;
  }

  /**
   * @brief 设置新的读取限制长度
   * @param limit 新的最大可读取字节数
   */
  void setLimit(int64_t limit) {
    _limit = limit;
  }

  virtual int32_t read(void * buff, uint32_t length) {
    if (_limit < 0) {
      // 无限制，直接读取
      return _stream->read(buff, length);
    } else if (_limit == 0) {
      // 已达到限制，返回流结束
      return -1;
    } else {
      // 调整本次读取长度不超过剩余限制
      int64_t rd = _limit < length ? _limit : length;
      int32_t ret = _stream->read(buff, rd);
      if (ret > 0) {
        // 扣除已读取字节数
        _limit -= ret;
      }
      return ret;
    }
  }
};

/**
 * @class ChecksumInputStream
 * @brief 带校验和计算的输入流装饰器
 * 
 * 在读取数据的同时自动计算数据的校验和，支持多种校验算法，用于数据完整性检查。
 */
class ChecksumInputStream : public FilterInputStream {
protected:
  ChecksumType _type;     // 校验和算法类型
  uint32_t _checksum;     // 当前累计计算得到的校验和
  int64_t _limit;         // 计算校验和的最大字节数限制
public:
  /**
   * @brief 构造函数，绑定输入流并设置校验算法
   * @param stream 底层输入流
   * @param type 校验和算法类型
   */
  ChecksumInputStream(InputStream * stream, ChecksumType type);

  virtual ~ChecksumInputStream() {
  }

  /**
   * @brief 获取当前校验和计算长度限制
   * @return 限制字节数
   */
  int64_t getLimit() {
    return _limit;
  }

  /**
   * @brief 设置校验和计算长度限制
   * @param limit 最大计算字节数
   */
  void setLimit(int64_t limit) {
    _limit = limit;
  }

  /**
   * @brief 重置校验和状态，重新开始计算
   */
  void resetChecksum();

  /**
   * @brief 获取当前计算得到的校验和
   * @return 校验和结果
   */
  uint32_t getChecksum();

  virtual int32_t read(void * buff, uint32_t length);
};

/**
 * @class ChecksumOutputStream
 * @brief 带校验和计算的输出流装饰器
 * 
 * 在写入数据的同时自动计算数据的校验和，支持多种校验算法，用于输出数据的完整性校验。
 */
class ChecksumOutputStream : public FilterOutputStream {
protected:
  ChecksumType _type;     // 校验和算法类型
  uint32_t _checksum;     // 当前累计计算得到的校验和
public:
  /**
   * @brief 构造函数，绑定输出流并设置校验算法
   * @param stream 底层输出流
   * @param type 校验和算法类型
   */
  ChecksumOutputStream(OutputStream * stream, ChecksumType type);

  virtual ~ChecksumOutputStream() {
  }

  /**
   * @brief 重置校验和状态，重新开始计算
   */
  void resetChecksum();

  /**
   * @brief 获取当前计算得到的校验和
   * @return 校验和结果
   */
  uint32_t getChecksum();

  virtual void write(const void * buff, uint32_t length);

};

} // namespace NativeTask

#endif /* STREAMS_H_ */