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
 * @file Buffers.cc
 * Hadoop MapReduce 本地任务IO缓冲区实现，提供带压缩支持的读写缓冲功能
 * 为本地任务处理提供高效的内存缓冲，支持输入解压缩和输出压缩
 */

#include <string>

#include "lib/commons.h"
#include "util/StringUtil.h"
#include "util/WritableUtils.h"
#include "lib/Buffers.h"

namespace NativeTask {

/**
 * 输入读取缓冲区，支持从输入流预读数据，支持压缩输入流自动解压缩
 * 为MapReduce本地任务提供高效的分块读取能力
 */
ReadBuffer::ReadBuffer()
    : _buff(NULL), _remain(0), _size(0), _capacity(0), _stream(NULL), _source(NULL) {
}

/**
 * 初始化读取缓冲区
 * @param size 缓冲区初始容量大小
 * @param stream 底层输入流
 * @param codec 压缩编码格式，如果为空则不启用解压缩
 */
void ReadBuffer::init(uint32_t size, InputStream * stream, const string & codec) {
  if (size < 1024) {
    THROW_EXCEPTION_EX(UnsupportException, "ReadBuffer size %u not support.", size);
  }
  _buff = (char *)malloc(size);
  if (NULL == _buff) {
    THROW_EXCEPTION(OutOfMemoryException, "create append buffer");
  }
  _capacity = size;
  _remain = 0;
  _size = 0;
  _stream = stream;
  _source = _stream;
  if (codec.length() > 0) {
    if (!Compressions::support(codec)) {
      THROW_EXCEPTION(UnsupportException, "compression codec not support");
    }
    _source = Compressions::getDecompressionStream(codec, _stream, size);
  }
}

/**
 * 销毁读取缓冲区，释放内存和压缩流资源
 */
ReadBuffer::~ReadBuffer() {
  if (_source != _stream) {
    delete _source;
    _source = NULL;
  }
  if (NULL != _buff) {
    free(_buff);
    _buff = NULL;
    _capacity = 0;
    _remain = 0;
    _size = 0;
  }
}

/**
 * 填充缓冲区并获取指定长度数据的指针
 * 若容量不足则自动扩容，保证能容纳指定长度数据
 * @param count 需要读取的数据长度
 * @return 读取到的数据首地址指针
 */
char * ReadBuffer::fillGet(uint32_t count) {

  if (unlikely(count > _capacity)) {
    // 计算新容量，取当前容量的2倍和需求中的较大值
    uint32_t newcap = _capacity * 2 > count ? _capacity * 2 : count;
    char * newbuff = (char*)malloc(newcap);

    if (newbuff == NULL) {
      THROW_EXCEPTION(OutOfMemoryException,
          StringUtil::Format("buff realloc failed, size=%u", newcap));
    }

    // 复制已有剩余数据到新缓冲区
    if (_remain > 0) {
      memcpy(newbuff, current(), _remain);
    }
    if (NULL != _buff) {
      free(_buff);
    }

    _buff = newbuff;
    _capacity = newcap;
  } else {
    // 移动剩余数据到缓冲区开头，腾出空间继续读取
    if (_remain > 0) {
      memmove(_buff, current(), _remain);
    }
  }
  _size = _remain;
  // 持续读取直到满足需求长度
  while (_remain < count) {
    int32_t rd = _source->read(_buff + _size, _capacity - _size);
    if (rd <= 0) {
      THROW_EXCEPTION(IOException, "read reach EOF");
    }
    _remain += rd;
    _size += rd;
  }
  char * ret = current();
  _remain -= count;
  return ret;
}

/**
 * 填充缓冲区并读取指定长度数据到目标内存
 * @param buff 目标内存地址
 * @param len 需要读取的数据长度
 * @return 实际读取长度，-1表示EOF
 */
int32_t ReadBuffer::fillRead(char * buff, uint32_t len) {
  uint32_t cp = _remain;
  if (cp > 0) {
    memcpy(buff, current(), cp);
    _remain = 0;
  }
  // TODO: read to buffer first
  int32_t ret = _source->readFully(buff + cp, len - cp);
  if (ret < 0 && cp == 0) {
    return ret;
  } else {
    return ret < 0 ? cp : ret + cp;
  }
}

/**
 * 读取Hadoop序列化格式的VLong变长整型
 * @return 解析得到的64位整型值
 */
int64_t ReadBuffer::fillReadVLong() {
  // 如果缓冲无数据，先填充
  if (_remain == 0) {
    int32_t rd = _source->read(_buff, _capacity);
    if (rd <= 0) {
      THROW_EXCEPTION(IOException, "fillReadVLong reach EOF");
    }
    _remain = rd;
    _size = rd;
  }
  int8_t * pos = (int8_t*)current();
  // 小值直接编码在第一个字节中，直接返回
  if (*pos >= -112) {
    _remain--;
    return (int64_t)*pos;
  }
  // 根据首字节判断符号和字节长度
  bool neg = *pos < -120;
  uint32_t len = neg ? (-119 - *pos) : (-111 - *pos);
  pos = (int8_t*)get(len);
  const int8_t * end = pos + len;
  uint64_t value = 0;
  // 逐字节组装得到数值
  while (++pos < end) {
    value = (value << 8) | *(uint8_t*)pos;
  }
  return neg ? (value ^ -1LL) : value;
}

///////////////////////////////////////////////////////////

/**
 * 输出追加缓冲区，支持向输出流批量写入数据，支持输出自动压缩
 * 为MapReduce本地任务提供高效的分块输出能力
 */
AppendBuffer::AppendBuffer()
    : _buff(NULL), _remain(0), _capacity(0), _counter(0), _stream(NULL), _dest(NULL),
        _compression(false) {
}

/**
 * 初始化输出追加缓冲区
 * @param size 缓冲区初始容量大小
 * @param stream 底层输出流
 * @param codec 压缩编码格式，如果为空则不启用压缩
 */
void AppendBuffer::init(uint32_t size, OutputStream * stream, const string & codec) {
  if (size < 1024) {
    THROW_EXCEPTION_EX(UnsupportException, "AppendBuffer size %u not support.", size);
  }
  _buff = (char *)malloc(size + 8);
  if (NULL == _buff) {
    THROW_EXCEPTION(OutOfMemoryException, "create append buffer");
  }
  _capacity = size;
  _remain = _capacity;
  _stream = stream;
  _dest = _stream;
  if (codec.length() > 0) {
    if (!Compressions::support(codec)) {
      THROW_EXCEPTION(UnsupportException, "compression codec not support");
    }
    _dest = Compressions::getCompressionStream(codec, _stream, size);
    _compression = true;
  }
}

/**
 * 获取压缩流对象，用于flush等操作
 * @return 压缩流指针，未启用压缩则返回NULL
 */
CompressStream * AppendBuffer::getCompressionStream() {
  if (_compression) {
    return (CompressStream *)_dest;
  } else {
    return NULL;
  }
}

/**
 * 销毁输出缓冲区，释放内存和压缩流资源
 */
AppendBuffer::~AppendBuffer() {
  if (_dest != _stream) {
    delete _dest;
    _dest = NULL;
  }
  if (NULL != _buff) {
    free(_buff);
    _buff = NULL;
    _remain = 0;
    _capacity = 0;
  }
}

/**
 * 将缓冲区中已积累的数据刷新到底层输出流
 * 重置剩余空间为全容量
 */
void AppendBuffer::flushd() {
  _dest->write(_buff, _capacity - _remain);
  _counter += _capacity - _remain;
  _remain = _capacity;
}

/**
 * 写入数据内部实现，缓冲区满时自动flush
 * 大数据块直接写入底层流，小数据块先累积到缓冲区
 * @param data 待写入数据地址
 * @param len 待写入数据长度
 */
void AppendBuffer::write_inner(const void * data, uint32_t len) {
  flushd();
  if (len >= _capacity / 2) {
    _dest->write(data, len);
    _counter += len;
  } else {
    simple_memcpy(_buff, data, len);
    _remain -= len;
  }
}

/**
 * 写入VLong变长整型内部实现
 * @param v 待写入的64位整型值
 */
void AppendBuffer::write_vlong_inner(int64_t v) {
  // 剩余空间不足时先flush
  if (_remain < 9) {
    flushd();
  }
  uint32_t len;
  // 序列化到缓冲区，更新剩余空间
  WritableUtils::WriteVLong(v, current(), len);
  _remain -= len;
}

/**
 * 连续写入两个VLong变长无符号整型内部实现
 * @param v1 第一个待写入值
 * @param v2 第二个待写入值
 */
void AppendBuffer::write_vuint2_inner(uint32_t v1, uint32_t v2) {
  // 剩余空间不足时先flush
  if (_remain < 10) {
    flushd();
  }
  uint32_t len;
  // 序列化第一个值，更新剩余空间
  WritableUtils::WriteVLong(v1, current(), len);
  _remain -= len;
  // 序列化第二个值，更新剩余空间
  WritableUtils::WriteVLong(v2, current(), len);
  _remain -= len;
}

} // namespace NativeTask