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
 * @file MCollectorOutputHandler.h
 * @brief 原生Map任务输出收集处理器，处理Map阶段输出键值对并写入Map输出收集器
 * 
 * 属于Hadoop MapReduce原生任务模块，负责对接Map输出收集器，处理输出数据的分发和缓存
 */

#ifndef MCOLLECTOROUTPUTHANDLER_H_
#define MCOLLECTOROUTPUTHANDLER_H_

#include "BatchHandler.h"
#include "lib/SpillOutputService.h"
#include "AbstractMapHandler.h"

namespace NativeTask {

/**
 * @class MCollectorOutputHandler
 * @brief Map任务输出处理处理器，将Map输出的键值对写入Map输出收集器
 * 
 * 核心职责是接收Map任务处理后的输出键值对，分配缓存空间，并转发给MapOutputCollector
 * 支持处理大键值对，处理不同字节序的输出数据，是原生Map任务输出流程的核心环节
 */
class MapOutputCollector;

class MCollectorOutputHandler : public AbstractMapHandler {
private:
  /// 固定大小键值对缓存容器
  FixSizeContainer _kvContainer;

  /// 关联的Map输出收集器实例，负责最终收集和排序Map输出
  MapOutputCollector * _collector;
  /// 大键值对处理目的缓冲区指针
  char * _dest;

  /// 存储字节序标识，处理不同平台字节序转换
  Endium _endium;

public:
  /**
   * @brief 构造函数
   */
  MCollectorOutputHandler();

  /**
   * @brief 析构函数
   */
  virtual ~MCollectorOutputHandler();

  /**
   * @brief 配置处理器，从配置中初始化参数和资源
   * @param config 配置对象指针
   */
  virtual void configure(Config * config);

  /**
   * @brief 完成输出处理，触发刷写溢出等收尾工作
   */
  virtual void finish();

  /**
   * @brief 处理输入缓冲区中的Map输出键值对，转发给收集器
   * @param byteBuffer 输入字节缓冲区，包含待处理的Map输出键值对
   */
  virtual void handleInput(ByteBuffer & byteBuffer);

private:
  /**
   * @brief 为指定分区的键值对分配缓存空间
   * @param partition 分区编号
   * @param kvlength 键值对总长度
   * @return 分配好的键值对缓冲区指针
   */
  KVBuffer * allocateKVBuffer(uint32_t partition, uint32_t kvlength);
};

}

#endif /* MCOLLECTOROUTPUTHANDLER_H_ */