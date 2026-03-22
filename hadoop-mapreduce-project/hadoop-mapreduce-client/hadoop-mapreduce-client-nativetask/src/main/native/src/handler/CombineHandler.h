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
 * @file CombineHandler.h
 * @brief  MapReduce本地任务阶段Combine处理处理器，负责在本地执行Map输出的合并操作
 * 
 * 该文件属于Hadoop MapReduce本地任务模块，提供Java侧Combine逻辑调用的处理能力，
 * 将Map输出的键值对分组后传递给Java Combiner合并，再将合并结果写出。
 */

#ifndef _COMBINEHANDLER_H_
#define _COMBINEHANDLER_H_

#include "lib/Combiner.h"
#include "BatchHandler.h"

namespace NativeTask {

/**
 * @brief 序列化框架类型枚举，标识键值对使用的序列化方式
 */
enum SerializationFramework {
  WRITABLE_SERIALIZATION = 0,  // Hadoop Writable序列化
  NATIVE_SERIALIZATION = 1     // 原生二进制序列化
};

/**
 * @brief 序列化信息存储结构，保存序列化后的键/值数据和长度信息
 */
struct SerializeInfo {
  Buffer buffer;          // 存储序列化后数据的缓冲区
  uint32_t outerLength;   // 数据总长度
  char varBytes[8];       // 可变长度数据起始空间
};

/**
 * @class CombineHandler
 * @brief Combine操作处理器，实现本地Combine运行接口和批量处理接口
 * 
 * 核心职责：接收Map阶段输出的键值对，调用Java Combiner进行本地合并，
 * 减少Reduce阶段需要处理的数据量，是MapReduce优化中重要的本地聚合步骤
 */
class CombineHandler : public NativeTask::ICombineRunner, public NativeTask::BatchHandler {
public:
  static const Command COMBINE;

private:

  CombineContext * _combineContext;  // Combine执行上下文
  KVIterator * _kvIterator;           // 待合并键值对迭代器
  IFileWriter * _writer;              // 合并结果写出器
  SerializeInfo _key;                 // 当前处理的key序列化信息
  SerializeInfo _value;               // 当前处理的value序列化信息

  KeyValueType _kType;                // Key类型标识
  KeyValueType _vType;                // Value类型标识
  MapOutputSpec _mapOutputSpec;       // Map输出规格描述
  Config * _config;                   // 配置对象指针
  bool _kvCached;                     // 键值对是否已缓存标记

  uint32_t _combineInputRecordCount;  // Combine输入记录数统计
  uint32_t _combineInputBytes;        // Combine输入字节数统计

  uint32_t _combineOutputRecordCount; // Combine输出记录数统计
  uint32_t _combineOutputBytes;       // Combine输出字节数统计

  FixSizeContainer _asideBuffer;      // 临时固定大小缓冲区
  ByteArray _asideBytes;              // 临时字节数组存储

public:
  /**
   * @brief 构造函数
   */
  CombineHandler();
  /**
   * @brief 析构函数，释放内部资源
   */
  virtual ~CombineHandler();

  /**
   * @brief 处理输入的Combine数据
   * @param byteBuffer 输入数据缓冲区
   */
  virtual void handleInput(ByteBuffer & byteBuffer);
  /**
   * @brief 完成Combine处理，输出最终结果
   */
  void finish();

  /**
   * @brief RPC调用处理入口，处理Combine命令
   * @param command 调用命令
   * @param param 参数缓冲区
   * @return 调用结果缓冲区
   */
  ResultBuffer * onCall(const Command& command, ParameterBuffer * param);

  /**
   * @brief 配置初始化，根据配置设置处理器参数
   * @param config 配置对象
   */
  void configure(Config * config);

  /**
   * @brief 核心Combine执行方法，对输入键值对执行合并
   * @param context Combine上下文
   * @param kvIterator 待合并键值对迭代器
   * @param writer 合并结果写出器
   */
  void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer);

  /**
   * @brief 数据加载完成回调，处理加载好的输入数据
   */
  virtual void onLoadData();

private:
  /**
   * @brief 将合并后的数据刷新到结果写出器
   */
  void flushDataToWriter();
  /**
   * @brief 输出键或值到结果，根据类型处理序列化
   * @param info 序列化信息存储
   * @param type 键/值类型
   */
  void outputKeyOrValue(SerializeInfo & info, KeyValueType type);
  /**
   * @brief 获取下一个待合并的键值对
   * @param key 存储key信息
   * @param value 存储value信息
   * @return 是否还有下一个键值对
   */
  bool nextKeyValue(SerializeInfo & key, SerializeInfo & value);
  /**
   * @brief 将待合并数据传递给Java Combiner处理
   * @param serializationType 使用的序列化框架类型
   * @return 处理后的输出字节数
   */
  uint32_t feedDataToJava(SerializationFramework serializationType);
  /**
   * @brief 使用Writable序列化方式将数据传递给Java Combiner
   * @return 处理后的输出字节数
   */
  uint32_t feedDataToJavaInWritableSerialization();
  /**
   * @brief 写入指定长度数据到结果输出
   * @param buf 数据缓冲区
   * @param length 数据长度
   */
  void write(char * buf, uint32_t length);

};

} /* namespace NativeTask */
#endif /* _JAVACOMBINEHANDLER_H_ */