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
 * @file CombineHandler.cc
 * @brief 原生MapReduce任务Combiner处理器实现，对接Java层Combiner逻辑，处理Map输出的合并操作
 *
 * 负责将Map任务输出的键值对整理后传递给Java层Combiner，再将合并结果写回供后续阶段使用
 */
#include "CombineHandler.h"

namespace NativeTask {

/** "refill"命令字符串常量 */
const char * REFILL = "refill";
/** "refill"字符串长度常量 */
const int LENGTH_OF_REFILL_STRING = 6;

/** Combine命令定义，命令ID为4 */
const Command CombineHandler::COMBINE(4, "Combine");

/**
 * @brief 构造函数，初始化成员变量默认值
 */
CombineHandler::CombineHandler()
    : _combineContext(NULL), _kvIterator(NULL), _writer(NULL), _kType(UnknownType),
        _vType(UnknownType), _config(NULL), _kvCached(false), _combineInputRecordCount(0),
        _combineInputBytes(0), _combineOutputRecordCount(0), _combineOutputBytes(0) {
}

/**
 * @brief 析构函数
 */
CombineHandler::~CombineHandler() {
}

/**
 * @brief 配置CombineHandler，从配置中读取Map输出键值类型信息
 * @param config 配置对象指针
 */
void CombineHandler::configure(Config * config) {

  _config = config;
  // 从配置解析Map输出规格
  MapOutputSpec::getSpecFromConfig(_config, _mapOutputSpec);
  // 提取键和值的序列化类型
  _kType = _mapOutputSpec.keyType;
  _vType = _mapOutputSpec.valueType;
}

/**
 * @brief 使用Writable序列化方式将键值对数据填充到输出缓冲区传给Java层
 * @return 写入的总字节数
 */
uint32_t CombineHandler::feedDataToJavaInWritableSerialization() {

  uint32_t written = 0;
  bool firstKV = true;
  // 重置输出缓冲区位置
  _out.position(0);

  // 处理上一轮缓存未发送的键值对
  if (_kvCached) {
    // 计算当前键值对总长度
    uint32_t kvLength = _key.outerLength + _value.outerLength + KVBuffer::headerLength();
    // 写入大端序编码的键长度和值长度
    outputInt(bswap(_key.outerLength));
    outputInt(bswap(_value.outerLength));
    // 写入键和值数据
    outputKeyOrValue(_key, _kType);
    outputKeyOrValue(_value, _vType);

    written += kvLength;
    // 清除缓存标记
    _kvCached = false;
    firstKV = false;
  }

  uint32_t recordCount = 0;
  // 循环读取下一个键值对
  while (nextKeyValue(_key, _value)) {

    _kvCached = false;
    recordCount++;

    uint32_t kvLength = _key.outerLength + _value.outerLength + KVBuffer::headerLength();

    // 缓冲区剩余空间不足容纳新键值对，缓存当前键值对，跳出循环
    if (!firstKV && kvLength > _out.remain()) {
      _kvCached = true;
      break;
    } else {
      firstKV = false;
      // 写入大端序编码的键长度和值长度
      outputInt(bswap(_key.outerLength));
      outputInt(bswap(_value.outerLength));
      // 写入键和值数据
      outputKeyOrValue(_key, _kType);
      outputKeyOrValue(_value, _vType);

      written += kvLength;
    }
  }

  // 缓冲区有数据则刷新输出到Java层
  if (_out.position() > 0) {
    flushOutput();
  }

  // 更新输入统计信息
  _combineInputRecordCount += recordCount;
  _combineInputBytes += written;
  return written;
}

/**
 * @brief 根据类型输出键或值数据，处理不同序列化格式
 * @param KV 键或值的序列化信息
 * @param type 键值类型
 */
void CombineHandler::outputKeyOrValue(SerializeInfo & KV, KeyValueType type) {
  switch (type) {
  case TextType:
    // Text类型：先输出VInt长度编码，再输出实际数据
    output(KV.varBytes, KV.outerLength - KV.buffer.length());
    output(KV.buffer.data(), KV.buffer.length());
    break;
  case BytesType:
    // Bytes类型：先输出固定4字节长度，再输出实际数据
    outputInt(bswap(KV.buffer.length()));
    output(KV.buffer.data(), KV.buffer.length());
    break;
  default:
    // 其他类型：直接输出二进制数据
    output(KV.buffer.data(), KV.buffer.length());
    break;
  }
}

/**
 * @brief 从迭代器读取下一个键值对，计算其外部总长度
 * @param[out] key 输出键信息
 * @param[out] value 输出值信息
 * @return 是否成功读取到下一个键值对
 */
bool CombineHandler::nextKeyValue(SerializeInfo & key, SerializeInfo & value) {

  // 从迭代器获取键值对缓冲
  if (!_kvIterator->next(key.buffer, value.buffer)) {
    return false;
  }

  uint32_t varLength = 0;
  // 根据键类型计算外部总长度
  switch (_kType) {
  case TextType:
    // Text类型写入VInt长度编码，总长度 = 数据长度 + VInt编码长度
    WritableUtils::WriteVInt(key.buffer.length(), key.varBytes, varLength);
    key.outerLength = key.buffer.length() + varLength;
    break;
  case BytesType:
    // Bytes类型总长度 = 数据长度 + 4字节固定长度头
    key.outerLength = key.buffer.length() + 4;
    break;
  default:
    // 其他类型总长度等于数据长度
    key.outerLength = key.buffer.length();
    break;
  }

  // 根据值类型计算外部总长度
  uint32_t varValueLength = 0;
  switch (_vType) {
  case TextType:
    WritableUtils::WriteVInt(value.buffer.length(), value.varBytes, varValueLength);
    value.outerLength = value.buffer.length() + varValueLength;
    break;
  case BytesType:
    value.outerLength = value.buffer.length() + 4;
    break;
  default:
    value.outerLength = value.buffer.length();
    break;
  }

  return true;
}

/**
 * @brief 根据序列化框架类型分发数据填充请求
 * @param serializationType 序列化框架类型
 * @return 写入的总字节数
 */
uint32_t CombineHandler::feedDataToJava(SerializationFramework serializationType) {
  if (serializationType == WRITABLE_SERIALIZATION) {
    return feedDataToJavaInWritableSerialization();
  }
  // 当前仅支持Writable序列化
  THROW_EXCEPTION(IOException, "Native Serialization not supported");
}

/**
 * @brief 处理从Java层传入的Combiner输出数据，处理跨缓冲区的不完整键值对
 * @param in 输入缓冲区
 */
void CombineHandler::handleInput(ByteBuffer & in) {
  char * buff = in.current();
  uint32_t length = in.remain();
  uint32_t remain = length;
  char * pos = buff;
  // 先填充之前未完成的临时缓冲区
  if (_asideBuffer.remain() > 0) {
    uint32_t filledLength = _asideBuffer.fill(pos, length);
    pos += filledLength;
    remain -= filledLength;
  }

  // 临时缓冲区已经填满，输出整键值对
  if (_asideBuffer.size() > 0 && _asideBuffer.remain() == 0) {
    _asideBuffer.position(0);
    write(_asideBuffer.current(), _asideBuffer.size());
    _asideBuffer.wrap(NULL, 0);
  }

  // 所有数据都已经填充到临时缓冲区，直接返回
  if (remain == 0) {
    return;
  }
  // 解析当前位置的KV头
  KVBuffer * kvBuffer = (KVBuffer *)pos;

  // 剩余数据不足KV头长度，数据不完整
  if (unlikely(remain < kvBuffer->headerLength())) {
    THROW_EXCEPTION(IOException, "k/v meta information incomplete");
  }

  // 转换字节序得到KV总长度
  uint32_t kvLength = kvBuffer->lengthConvertEndium();

  // 当前缓冲区剩余数据不足整个KV，分配临时缓冲区存储完整KV
  if (kvLength > remain) {
    _asideBytes.resize(kvLength);
    _asideBuffer.wrap(_asideBytes.buff(), _asideBytes.size());
    _asideBuffer.fill(pos, remain);
    pos += remain;
    remain = 0;
  } else {
    // 剩余数据足够，直接写入
    write(pos, remain);
  }
}

/**
 * @brief 解析并写入批量键值对到输出写入器
 * @param buf 数据缓冲区
 * @param length 数据总长度
 */
void CombineHandler::write(char * buf, uint32_t length) {
  KVBuffer * kv = NULL;
  char * pos = buf;
  uint32_t remain = length;

  uint32_t outputRecordCount = 0;
  // 循环解析所有完整键值对
  while (remain > 0) {
    kv = (KVBuffer *)pos;
    // 转换键长度和值长度的字节序
    kv->keyLength = bswap(kv->keyLength);
    kv->valueLength = bswap(kv->valueLength);
    // 写入到最终输出
    _writer->write(kv->getKey(), kv->keyLength, kv->getValue(), kv->valueLength);
    outputRecordCount++;
    // 更新位置和剩余长度
    remain -= kv->length();
    pos += kv->length();
  }

  // 更新输出统计信息
  _combineOutputRecordCount += outputRecordCount;
  _combineOutputBytes += length;
}

/**
 * @brief 将32位整数转换为4字节字符串
 * @param length 输入整数
 * @return 二进制字符串
 */
string toString(uint32_t length) {
  string result;
  result.reserve(4);
  result.assign((char *)(&length), 4);
  return result;
}

/**
 * @brief 加载数据时触发，将数据填充传给Java层Combiner
 */
void CombineHandler::onLoadData() {
  feedDataToJava(WRITABLE_SERIALIZATION);
}

/**
 * @brief 处理不支持的命令调用
 * @param command 命令对象
 * @param param 参数缓冲区
 * @return 永远抛出异常不返回
 */
ResultBuffer * CombineHandler::onCall(const Command& command, ParameterBuffer * param) {
  THROW_EXCEPTION(UnsupportException, "Command not supported by RReducerHandler");
}

/**
 * @brief 执行Combine合并操作主入口
 * @param type Combine上下文
 * @param kvIterator 输入键值对迭代器
 * @param writer 合并结果写入器
 */
void CombineHandler::combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer) {

  // 重置统计计数器
  _combineInputRecordCount = 0;
  _combineOutputRecordCount = 0;
  _combineInputBytes = 0;
  _combineOutputBytes = 0;

  // 保存上下文引用
  this->_combineContext = &type;
  this->_kvIterator = kvIterator;
  this->_writer = writer;
  // 调用Java层Combine逻辑
  call(COMBINE, NULL);

  // 输出Combine统计日志
  LOG("[CombineHandler] input Record Count: %d, input Bytes: %d, "
      "output Record Count: %d, output Bytes: %d",
      _combineInputRecordCount, _combineInputBytes,
      _combineOutputRecordCount, _combineOutputBytes);
  return;
}

/**
 * @brief 完成Combine操作，此处无额外清理逻辑
 */
void CombineHandler::finish() {
}

} /* namespace NativeTask */