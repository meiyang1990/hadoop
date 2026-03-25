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
 * @file GzipCodec.cc
 * Gzip压缩解压编解码器实现，提供基于zlib的原生Gzip压缩/解压流处理，
 * 用于MapReduce本地任务中的Gzip格式压缩数据处理
 */

#include <zconf.h>
#include <zlib.h>
#include "lib/commons.h"
#include "GzipCodec.h"
#include <iostream>

namespace NativeTask {

/**
 * Gzip压缩流构造函数，初始化zlib压缩上下文和缓冲区
 * @param stream 底层输出流，压缩后数据写入此处
 * @param bufferSizeHint 压缩缓冲区大小提示
 */
GzipCompressStream::GzipCompressStream(OutputStream * stream, uint32_t bufferSizeHint)
    : CompressStream(stream), _compressedBytesWritten(0), _zstream(NULL), _finished(false) {
  // 分配压缩缓冲区
  _buffer = new char[bufferSizeHint];
  _capacity = bufferSizeHint;
  // 分配zlib流结构内存
  _zstream = malloc(sizeof(z_stream));
  z_stream * zstream = (z_stream*)_zstream;
  memset(zstream, 0, sizeof(z_stream));
  // 初始化gzip压缩，设置31的窗口大小用于生成gzip格式头
  if (Z_OK != deflateInit2(zstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 31, 8,
      Z_DEFAULT_STRATEGY)) {
    free(_zstream);
    _zstream = NULL;
    THROW_EXCEPTION(IOException, "deflateInit2 failed");
  }
  // 设置输出缓冲区位置
  zstream->next_out = (Bytef *)_buffer;
  zstream->avail_out = _capacity;
}

/**
 * Gzip压缩流析构函数，释放zlib上下文和缓冲区资源
 */
GzipCompressStream::~GzipCompressStream() {
  if (_zstream != NULL) {
    deflateEnd((z_stream*)_zstream);
    free(_zstream);
    _zstream = NULL;
  }
  delete[] _buffer;
  _buffer = NULL;
}

/**
 * 写入待压缩数据，进行流式压缩处理
 * @param buff 待压缩数据缓冲区
 * @param length 待压缩数据长度
 */
void GzipCompressStream::write(const void * buff, uint32_t length) {
  z_stream * zstream = (z_stream*)_zstream;
  // 设置输入数据位置和长度
  zstream->next_in = (Bytef*)buff;
  zstream->avail_in = length;
  // 循环压缩直到所有输入处理完成
  while (true) {
    int ret = deflate(zstream, Z_NO_FLUSH);
    if (ret == Z_OK) {
      // 输出缓冲区已满，写入底层流并重置缓冲区
      if (zstream->avail_out == 0) {
        _stream->write(_buffer, _capacity);
        _compressedBytesWritten += _capacity;
        zstream->next_out = (Bytef *)_buffer;
        zstream->avail_out = _capacity;
      }
      // 所有输入处理完成，退出循环
      if (zstream->avail_in == 0) {
        break;
      }
    } else {
      THROW_EXCEPTION(IOException, "deflate return error");
    }
  }
  _finished = false;
}

/**
 * 刷新压缩流，完成当前压缩块并写入所有剩余压缩数据
 */
void GzipCompressStream::flush() {
  z_stream * zstream = (z_stream*)_zstream;
  // 循环完成压缩
  while (true) {
    int ret = deflate(zstream, Z_FINISH);
    if (ret == Z_OK) {
      // 输出缓冲区已满，写入底层流并重置
      if (zstream->avail_out == 0) {
        _stream->write(_buffer, _capacity);
        _compressedBytesWritten += _capacity;
        zstream->next_out = (Bytef *)_buffer;
        zstream->avail_out = _capacity;
      } else {
        THROW_EXCEPTION(IOException, "flush state error");
      }
    } else if (ret == Z_STREAM_END) {
      // 压缩完成，写入剩余未输出数据
      size_t wt = zstream->next_out - (Bytef*)_buffer;
      _stream->write(_buffer, wt);
      _compressedBytesWritten += wt;
      zstream->next_out = (Bytef *)_buffer;
      zstream->avail_out = _capacity;
      break;
    }
  }
  _finished = true;
  _stream->flush();
}

/**
 * 重置压缩流状态，可复用压缩上下文
 */
void GzipCompressStream::resetState() {
  z_stream * zstream = (z_stream*)_zstream;
  deflateReset(zstream);
}

/**
 * 关闭压缩流，若未完成压缩则先执行刷新
 */
void GzipCompressStream::close() {
  if (!_finished) {
    flush();
  }
}

/**
 * 直接写入原始数据到输出流，不进行压缩（用于未压缩块）
 * @param buff 原始数据缓冲区
 * @param length 原始数据长度
 */
void GzipCompressStream::writeDirect(const void * buff, uint32_t length) {
  if (!_finished) {
    flush();
  }
  _stream->write(buff, length);
  _compressedBytesWritten += length;
}

//////////////////////////////////////////////////////////////

/**
 * Gzip解压流构造函数，初始化zlib解压上下文和缓冲区
 * @param stream 底层输入流，待解压数据从此读取
 * @param bufferSizeHint 解压缓冲区大小提示
 */
GzipDecompressStream::GzipDecompressStream(InputStream * stream, uint32_t bufferSizeHint)
    : DecompressStream(stream), _compressedBytesRead(0), _zstream(NULL) {
  // 分配解压缓冲区
  _buffer = new char[bufferSizeHint];
  _capacity = bufferSizeHint;
  // 分配zlib流结构内存
  _zstream = malloc(sizeof(z_stream));
  z_stream * zstream = (z_stream*)_zstream;
  memset(zstream, 0, sizeof(z_stream));
  // 初始化解压上下文，设置31窗口大小支持gzip格式
  if (Z_OK != inflateInit2(zstream, 31)) {
    free(_zstream);
    _zstream = NULL;
    THROW_EXCEPTION(IOException, "inflateInit2 failed");
  }
  zstream->next_in = NULL;
  zstream->avail_in = 0;
  _eof = false;
}

/**
 * Gzip解压流析构函数，释放zlib上下文和缓冲区资源
 */
GzipDecompressStream::~GzipDecompressStream() {
  if (_zstream != NULL) {
    inflateEnd((z_stream*)_zstream);
    free(_zstream);
    _zstream = NULL;
  }
  delete[] _buffer;
  _buffer = NULL;
}

/**
 * 读取解压后的数据，进行流式解压处理
 * @param buff 输出缓冲区，存放解压后的数据
 * @param length 需要读取的解压后数据长度
 * @return 实际读取的解压后数据长度，-1表示流结束
 */
int32_t GzipDecompressStream::read(void * buff, uint32_t length) {
  z_stream * zstream = (z_stream*)_zstream;
  // 设置输出缓冲区位置和可用长度
  zstream->next_out = (Bytef*)buff;
  zstream->avail_out = length;
  // 循环解压直到输出缓冲区满或流结束
  while (true) {
    // 输入缓冲区已空，从底层流读取更多待解压数据
    if (zstream->avail_in == 0) {
      int32_t rd = _stream->read(_buffer, _capacity);
      if (rd <= 0) {
        _eof = true;
        size_t wt = zstream->next_out - (Bytef*)buff;
        return wt > 0 ? wt : -1;
      } else {
        _compressedBytesRead += rd;
        zstream->next_in = (Bytef*)_buffer;
        zstream->avail_in = rd;
      }
    }
    int ret = inflate(zstream, Z_NO_FLUSH);
    if (ret == Z_OK || ret == Z_STREAM_END) {
      // 输出缓冲区已满，返回请求长度
      if (zstream->avail_out == 0) {
        return length;
      }
    } else {
      return -1;
    }
  }
  return -1;
}

/**
 * 关闭解压流
 */
void GzipDecompressStream::close() {
}

/**
 * 直接从输入流读取原始压缩数据，不进行解压（用于读取未压缩块）
 * @param buff 输出缓冲区
 * @param length 需要读取的长度
 * @return 实际读取的长度，-1表示流结束
 */
int32_t GzipDecompressStream::readDirect(void * buff, uint32_t length) {
  int32_t ret = _stream->readFully(buff, length);
  if (ret > 0) {
    _compressedBytesRead += ret;
  }
  return ret;
}

} // namespace NativeTask