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
 * @file BufferStream.cc
 * @brief Hadoop NativeTask 本地内存缓冲流实现，提供基于堆内存的读写缓冲能力
 * 
 * 本文件属于Hadoop MapReduce本地任务模块，实现了输入输出内存缓冲类，
 * 为本地任务序列化/反序列化过程提供高效的内存读写支持。
 */

#include "lib/commons.h"
#include "lib/BufferStream.h"

namespace NativeTask {

/**
 * @class InputBuffer
 * @brief 输入内存缓冲类，提供从预分配内存缓冲中读取数据的能力
 * 
 * 用于包装已填充好数据的内存块，提供类似输入流的读接口，支持顺序读取。
 */

/**
 * 从输入缓冲读取指定长度数据到目标缓冲区
 * @param buff 目标缓冲区指针，用于存储读取到的数据
 * @param length 需要读取的字节长度
 * @return 实际读取的字节数，返回-1表示已经读到缓冲末尾无更多数据
 */
int32_t InputBuffer::read(void * buff, uint32_t length) {
  uint32_t rd = _capacity - _position < length ? _capacity - _position : length;
  if (rd > 0) {
    memcpy(buff, _buff + _position, rd);
    _position += rd;
    return rd;
  }
  return length == 0 ? 0 : -1;
}

/**
 * @class OutputBuffer
 * @brief 输出内存缓冲类，提供向预分配内存缓冲中写入数据的能力
 * 
 * 用于包装一块可写的内存块，提供类似输出流的写接口，支持顺序写入，
 * 超出缓冲容量会抛出IO异常。
 */

/**
 * 将源缓冲区数据写入到输出缓冲中
 * @param buff 源数据缓冲区指针，待写入的数据
 * @param length 需要写入的字节长度
 * @throws IOException 当剩余空间不足时抛出IO异常
 */
void OutputBuffer::write(const void * buff, uint32_t length) {
  if (_position + length <= _capacity) {
    memcpy(_buff + _position, buff, length);
    _position += length;
  } else {
    THROW_EXCEPTION(IOException, "OutputBuffer too small to write");
  }
}

} // namespace NativeTask