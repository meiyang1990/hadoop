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
 * @file MapOutputCollector.h
 * @brief 本地Map任务输出收集器头文件，属于Hadoop MapReduce本地任务模块
 *
 * 负责收集Map任务输出的<key, value>对，按分区缓存、排序并溢写磁盘，
 * 是MapReduce Shuffle阶段在Map端的核心处理组件
 */

#ifndef MAP_OUTPUT_COLLECTOR_H_
#define MAP_OUTPUT_COLLECTOR_H_

#include "NativeTask.h"
#include "lib/MemoryPool.h"
#include "util/Timer.h"
#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "lib/IFile.h"
#include "lib/SpillInfo.h"
#include "lib/Combiner.h"
#include "lib/PartitionBucket.h"
#include "lib/SpillOutputService.h"

namespace NativeTask {

/**
 * @struct SortMetrics
 * @brief 排序过程指标统计，记录排序记录数和耗时
 */
struct SortMetrics {
  uint64_t recordCount;
  uint64_t sortTime;

public:
  SortMetrics()
      : recordCount(0), sortTime(0) {
  }
};

/**
 * @class CombineRunnerWrapper
 * @brief Combiner执行器包装类，统一处理本地C++ Combiner和Java Combiner的创建与执行
 *
 * 封装了Combiner的初始化和执行逻辑，根据配置选择创建对应类型的执行器，
 * 负责在溢写前对Map输出进行局部聚合，减少溢写数据量
 */
class CombineRunnerWrapper : public ICombineRunner {
private:
  Config * _config;
  ICombineRunner * _combineRunner;
  bool _isJavaCombiner;
  bool _combinerInited;
  SpillOutputService * _spillOutput;

public:
  CombineRunnerWrapper(Config * config, SpillOutputService * service)
      : _config(config), _combineRunner(NULL), _isJavaCombiner(false),
          _combinerInited(false), _spillOutput(service) {
  }

  ~CombineRunnerWrapper() {
    if (!_isJavaCombiner) {
      delete _combineRunner;
    }
  }

  virtual void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer);

private:
  ICombineRunner * createCombiner();
};

/**
 * @class MapOutputCollector
 * @brief Map任务输出收集器核心类，负责收集、缓存、排序和溢写Map任务输出
 *
 * 核心职责：
 * 1. 按分区缓存Map输出的<key, value>对
 * 2. 内存不足时触发溢写，将数据排序后写入磁盘
 * 3. 可选地在溢写前执行Combiner局部聚合
 * 4. Map任务结束后对所有溢写进行最终合并
 */
class MapOutputCollector {
  // 默认最小块大小，单位字节
  static const uint32_t DEFAULT_MIN_BLOCK_SIZE = 16 * 1024;
  // 默认最大块大小，单位字节
  static const uint32_t DEFAULT_MAX_BLOCK_SIZE = 4 * 1024 * 1024;

private:
  Config * _config;

  // 分区总数
  uint32_t _numPartitions;
  // 分区桶数组，每个分区对应一个桶缓存数据
  PartitionBucket ** _buckets;

  // key比较器指针，用于排序
  ComparatorPtr _keyComparator;

  // Combiner执行器
  ICombineRunner * _combineRunner;

  // 输出记录计数器
  Counter * _mapOutputRecords;
  // 输出字节计数器
  Counter * _mapOutputBytes;
  // 物化后字节计数器
  Counter * _mapOutputMaterializedBytes;
  // 溢写记录计数器
  Counter * _spilledRecords;

  // 溢写输出服务，处理溢写文件创建与写入
  SpillOutputService * _spillOutput;

  // 默认每个分区块大小
  uint32_t _defaultBlockSize;

  // 溢写信息列表，记录所有溢写文件信息
  SpillInfos _spillInfos;

  // Map输出规格描述，包含序列化、压缩等配置
  MapOutputSpec _spec;

  // 收集阶段计时器，统计收集耗时
  Timer _collectTimer;

  // 内存池，用于分配缓存内存
  MemoryPool * _pool;

public:
  /**
   * @brief 构造函数
   * @param num_partition 分区总数
   * @param spillService 溢写输出服务实例
   */
  MapOutputCollector(uint32_t num_partition, SpillOutputService * spillService);

  ~MapOutputCollector();

  /**
   * @brief 根据配置初始化收集器
   * @param config 配置对象指针
   */
  void configure(Config * config);

  /**
   * 收集一条Map输出的<key, value>对
   * @param key key地址
   * @param keylen key长度
   * @param value value地址
   * @param vallen value长度
   * @param partitionId 分区编号
   * @return true 收集成功；false 缓存已满，需要触发溢写
   */
  bool collect(const void * key, uint32_t keylen, const void * value, uint32_t vallen,
      uint32_t partitionId);

  /**
   * @brief 为指定分区分配KV缓存空间
   * @param partitionId 分区编号
   * @param kvlength 需要分配的总长度
   * @return 分配得到的KV缓存指针
   */
  KVBuffer * allocateKVBuffer(uint32_t partitionId, uint32_t kvlength);

  /**
   * @brief 关闭收集器，触发最终溢写
   */
  void close();

private:
  /**
   * @brief 内部初始化方法
   * @param maxBlockSize 最大块大小
   * @param memory_capacity 总内存容量
   * @param keyComparator key比较器
   * @param combiner Combiner执行器
   */
  void init(uint32_t maxBlockSize, uint32_t memory_capacity, ComparatorPtr keyComparator,
      ICombineRunner * combiner);

  /**
   * @brief 重置收集器状态
   */
  void reset();

  /**
   * @brief 对指定范围分区桶进行排序，准备溢写
   * @param orderType 排序顺序
   * @param sortType 排序算法
   * @param writer IFile写入器
   * @param metrics 输出排序统计指标
   */
  void sortPartitions(SortOrder orderType, SortAlgorithm sortType, IFileWriter * writer,
      SortMetrics & metrics);

  /**
   * @brief 根据配置和输出规格获取key比较器
   * @param config 配置对象
   * @param spec Map输出规格
   * @return key比较器指针
   */
  ComparatorPtr getComparator(Config * config, MapOutputSpec & spec);

  /**
   * @brief 向上取整计算，按unit对齐
   * @param v 输入值
   * @param unit 对齐单位
   * @return 对齐后的值
   */
  inline uint32_t GetCeil(uint32_t v, uint32_t unit) {
    return ((v + unit - 1) / unit) * unit;
  }

  /**
   * @brief 计算默认每个分区块大小
   * @param memoryCapacity 总可用内存
   * @param partitionNum 分区总数
   * @param maxBlockSize 最大允许块大小
   * @return 计算得到的默认块大小
   */
  uint32_t getDefaultBlockSize(uint32_t memoryCapacity, uint32_t partitionNum,
      uint32_t maxBlockSize) {
    uint32_t defaultBlockSize = memoryCapacity / _numPartitions / 4;
    defaultBlockSize = GetCeil(defaultBlockSize, DEFAULT_MIN_BLOCK_SIZE);
    defaultBlockSize = std::min(defaultBlockSize, maxBlockSize);
    return defaultBlockSize;
  }

  /**
   * @brief 获取指定编号的分区桶
   * @param partition 分区编号
   * @return 分区桶指针
   */
  PartitionBucket * getPartition(uint32_t partition);

  /**
   * @brief 中间溢写，内存不足时触发，使用当前配置处理在存数据
   * @param spillOutput 溢写输出文件路径
   * @param indexFilePath 索引文件路径
   * @param final 是否为最终溢写
   */
  void middleSpill(const std::string & spillOutput, const std::string & indexFilePath, bool final);

  /**
   * @brief 最终溢写，Map任务输出全部收集完成后，合并所有溢写和内存数据
   * @param filepath 最终输出文件路径
   * @param indexpath 最终索引文件路径
   */
  void finalSpill(const std::string & filepath, const std::string & indexpath);
};

} //namespace NativeTask

#endif /* MAP_OUTPUT_COLLECTOR_H_ */