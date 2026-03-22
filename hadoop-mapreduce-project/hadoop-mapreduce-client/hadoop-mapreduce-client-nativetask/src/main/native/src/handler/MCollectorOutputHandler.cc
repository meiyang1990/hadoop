// 这个文件已经全部加上中文注释
/*
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
 * @file MCollectorOutputHandler.cc
 * MapReduce本地任务Map输出收集处理器，负责处理Java端发来的键值对数据，
 * 并写入对应分区的Map输出缓冲区，支持Combiner合并。
 */

#include "lib/commons.h"
#include "util/StringUtil.h"
#include "lib/TaskCounters.h"
#include "MCollectorOutputHandler.h"
#include "lib/NativeObjectFactory.h"
#include "lib/MapOutputCollector.h"
#include "CombineHandler.h"

using std::string;
using std::vector;

namespace NativeTask {

/**
 * @class MCollectorOutputHandler
 * @brief Map输出收集处理器，负责解析Java端发送的Map输出键值对，分配缓冲区并写入Map输出收集器
 * 
 * 核心职责：接收JNI传递过来的序列化键值对数据，解析分区、键长、值长信息，
 * 分配对应分区的输出缓冲区，将数据写入Map输出收集器，为后续Spill溢写做准备。
 */

MCollectorOutputHandler::MCollectorOutputHandler()
    : _collector(NULL), _dest(NULL), _endium(LARGE_ENDIUM) {
}

MCollectorOutputHandler::~MCollectorOutputHandler() {
  _dest = NULL;
  delete _collector;
  _collector = NULL;
}

/**
 * @brief 配置Map输出收集处理器，初始化Map输出收集器
 * @param config 配置对象，包含reduce数量等配置信息
 */
void MCollectorOutputHandler::configure(Config * config) {
  if (NULL == config) {
    return;
  }

  // 获取reduce任务数量，即分区数量
  uint32_t partition = config->getInt(MAPRED_NUM_REDUCES, 1);

  // 创建Map输出收集器，绑定当前处理器作为分配回调
  _collector = new MapOutputCollector(partition, this);
  _collector->configure(config);
}

/**
 * @brief 完成输出处理，关闭Map输出收集器
 */
void MCollectorOutputHandler::finish() {
  _collector->close();
  BatchHandler::finish();
}

/**
 * @brief 处理输入缓冲区中的序列化键值对数据，解析并写入对应分区
 * @param in 输入ByteBuffer，包含Java端发来的序列化键值对
 */
void MCollectorOutputHandler::handleInput(ByteBuffer & in) {
  char * buff = in.current();
  uint32_t length = in.remain();

  const char * end = buff + length;
  char * pos = buff;
  // 先填充未完成的键值对容器
  if (_kvContainer.remain() > 0) {
    uint32_t filledLength = _kvContainer.fill(pos, length);
    pos += filledLength;
  }

  // 循环处理所有完整键值对
  while (end - pos > 0) {
    // 将当前位置转换为带分区ID的键值对缓冲区
    KVBufferWithParititionId * kvBuffer = (KVBufferWithParititionId *)pos;

    // 检查元数据长度是否足够
    if (unlikely(end - pos < KVBuffer::headerLength())) {
      THROW_EXCEPTION(IOException, "k/v meta information incomplete");
    }

    // 如果是大端字节序，转换为本地字节序
    if (_endium == LARGE_ENDIUM) {
      kvBuffer->partitionId = bswap(kvBuffer->partitionId);
      kvBuffer->buffer.keyLength = bswap(kvBuffer->buffer.keyLength);
      kvBuffer->buffer.valueLength = bswap(kvBuffer->buffer.valueLength);
    }

    // 计算整个键值对总长度
    uint32_t kvLength = kvBuffer->buffer.length();

    // 为当前分区分配键值缓冲区
    KVBuffer * dest = allocateKVBuffer(kvBuffer->partitionId, kvLength);
    // 将容器包装到目标缓冲区
    _kvContainer.wrap((char *)dest, kvLength);

    pos += 4; // 跳过分区ID长度字段
    // 将剩余数据填充到容器中
    uint32_t filledLength = _kvContainer.fill(pos, end - pos);
    pos += filledLength;
  }
}

/**
 * @brief 分配指定分区的键值缓冲区，委托给Map输出收集器实现
 * @param partitionId 分区ID
 * @param kvlength 键值对总长度
 * @return 分配好的KV缓冲区指针
 */
KVBuffer * MCollectorOutputHandler::allocateKVBuffer(uint32_t partitionId, uint32_t kvlength) {
  KVBuffer * dest = _collector->allocateKVBuffer(partitionId, kvlength);
  return dest;
}

} // namespace NativeTask