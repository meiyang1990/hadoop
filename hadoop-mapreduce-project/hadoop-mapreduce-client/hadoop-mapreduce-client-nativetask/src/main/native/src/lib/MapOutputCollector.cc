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
 * @file MapOutputCollector.cc
 * @brief Hadoop native MapReduce Map端输出收集器实现
 * 
 * 负责在Map任务执行过程中，按分区收集、排序、溢写和合并Map输出结果，
 * 是native任务中Map输出处理的核心组件，替代Java版本实现提升性能。
 */

#include <string>

#include "lib/commons.h"
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "lib/FileSystem.h"
#include "lib/NativeObjectFactory.h"
#include "lib/MapOutputCollector.h"
#include "lib/Merge.h"
#include "NativeTask.h"
#include "util/WritableUtils.h"
#include "util/DualPivotQuickSort.h"
#include "lib/Combiner.h"
#include "lib/TaskCounters.h"
#include "lib/MinHeap.h"

namespace NativeTask {

/**
 * @brief 创建Combiner运行器实例
 * @return 返回创建好的Combiner运行器指针，无需Combiner时返回NULL
 */
ICombineRunner * CombineRunnerWrapper::createCombiner() {

  ICombineRunner * combineRunner = NULL;
  if (NULL != _config->get(NATIVE_COMBINER)) {
    // 此简化版本不再支持用户自定义native Combiner实现
    THROW_EXCEPTION_EX(UnsupportException, "Native Combiners not supported");
  }

  CombineHandler * javaCombiner = _spillOutput->getJavaCombineHandler();
  if (NULL != javaCombiner) {
    _isJavaCombiner = true;
    combineRunner = (ICombineRunner *)javaCombiner;
  } else {
    LOG("[MapOutputCollector::getCombiner] cannot get combine handler from java");
  }
  return combineRunner;
}

/**
 * @brief 执行Combine操作
 * @param type Combine上下文类型
 * @param kvIterator 键值对迭代器
 * @param writer 输出写入器
 */
void CombineRunnerWrapper::combine(CombineContext type, KVIterator * kvIterator,
    IFileWriter * writer) {

  if (!_combinerInited) {
    _combineRunner = createCombiner();
    _combinerInited = true;
  }

  if (NULL != _combineRunner) {
    _combineRunner->combine(type, kvIterator, writer);
  } else {
    LOG("[CombineRunnerWrapper::combine] no valid combiner");
  }
}

/////////////////////////////////////////////////////////////////
// MapOutputCollector
/////////////////////////////////////////////////////////////////

/**
 * @class MapOutputCollector
 * @brief Map任务输出收集器核心类
 * 
 * 负责按分区收集Map输出的键值对，管理内存缓冲区，
 * 在内存不足时触发溢写（spill）到本地磁盘，最终合并所有溢写文件生成最终输出，
 * 是native Map任务处理流程中输出管理的核心组件。
 */

/**
 * @brief 构造函数，初始化基础成员
 * @param numberPartitions 分区数量
 * @param spillService 溢写输出服务句柄
 */
MapOutputCollector::MapOutputCollector(uint32_t numberPartitions, SpillOutputService * spillService)
    : _config(NULL), _numPartitions(numberPartitions), _buckets(NULL),
      _keyComparator(NULL), _combineRunner(NULL),
      _mapOutputRecords(NULL), _mapOutputBytes(NULL),
      _mapOutputMaterializedBytes(NULL), _spilledRecords(NULL),
      _spillOutput(spillService), _defaultBlockSize(0), _pool(NULL) {
  _pool = new MemoryPool();
}

/**
 * @brief 析构函数，释放所有内存资源
 */
MapOutputCollector::~MapOutputCollector() {

  if (NULL != _buckets) {
    for (uint32_t i = 0; i < _numPartitions; i++) {
      if (NULL != _buckets[i]) {
        delete _buckets[i];
        _buckets[i] = NULL;
      }
    }
  }

  delete[] _buckets;
  _buckets = NULL;

  if (NULL != _pool) {
    delete _pool;
    _pool = NULL;
  }

  if (NULL != _combineRunner) {
    delete _combineRunner;
    _combineRunner = NULL;
  }
}

/**
 * @brief 初始化收集器，创建各分区存储桶和内存池
 * @param defaultBlockSize 默认块大小
 * @param memoryCapacity 总内存容量
 * @param keyComparator 键比较器
 * @param combiner Combiner运行器
 */
void MapOutputCollector::init(uint32_t defaultBlockSize, uint32_t memoryCapacity,
    ComparatorPtr keyComparator, ICombineRunner * combiner) {

  this->_combineRunner = combiner;

  this->_defaultBlockSize = defaultBlockSize;

  _pool->init(memoryCapacity);

  // TODO: add support for customized comparator
  this->_keyComparator = keyComparator;

  _buckets = new PartitionBucket*[_numPartitions];

  for (uint32_t partitionId = 0; partitionId < _numPartitions; partitionId++) {
    PartitionBucket * pb = new PartitionBucket(_pool, partitionId, keyComparator, _combineRunner,
        defaultBlockSize);

    _buckets[partitionId] = pb;
  }

  // 获取对应任务计数器引用
  _mapOutputRecords = NativeObjectFactory::GetCounter(
      TaskCounters::TASK_COUNTER_GROUP, TaskCounters::MAP_OUTPUT_RECORDS);
  _mapOutputBytes = NativeObjectFactory::GetCounter(
      TaskCounters::TASK_COUNTER_GROUP, TaskCounters::MAP_OUTPUT_BYTES);
  _mapOutputMaterializedBytes = NativeObjectFactory::GetCounter(
      TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::MAP_OUTPUT_MATERIALIZED_BYTES);
  _spilledRecords = NativeObjectFactory::GetCounter(
      TaskCounters::TASK_COUNTER_GROUP, TaskCounters::SPILLED_RECORDS);

  _collectTimer.reset();
}

/**
 * @brief 重置收集器状态，清空所有分区数据和内存池
 */
void MapOutputCollector::reset() {
  for (uint32_t i = 0; i < _numPartitions; i++) {
    if (NULL != _buckets[i]) {
      _buckets[i]->reset();
    }
  }
  _pool->reset();
}

/**
 * @brief 从配置加载参数并完成收集器配置
 * @param config 配置对象指针
 */
void MapOutputCollector::configure(Config * config) {
  _config = config;
  MapOutputSpec::getSpecFromConfig(config, _spec);

  uint32_t maxBlockSize = config->getInt(NATIVE_SORT_MAX_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE);
  uint32_t capacity = config->getInt(MAPRED_IO_SORT_MB, 300) * 1024 * 1024;

  uint32_t defaultBlockSize = getDefaultBlockSize(capacity, _numPartitions, maxBlockSize);
  LOG("Native Total MemoryBlockPool: num_partitions %u, min_block_size %uK, "
      "max_block_size %uK, capacity %uM", _numPartitions, defaultBlockSize / 1024,
      maxBlockSize / 1024, capacity / 1024 / 1024);

  ComparatorPtr comparator = getComparator(config, _spec);

  ICombineRunner * combiner = NULL;
  // 配置存在Combiner类，创建Combiner运行器包装类
  if (NULL != config->get(NATIVE_COMBINER)
      // config name for old api and new api
      || NULL != config->get(MAPRED_COMBINE_CLASS_OLD)
      || NULL != config->get(MAPRED_COMBINE_CLASS_NEW)) {
    combiner = new CombineRunnerWrapper(config, _spillOutput);
  }

  init(defaultBlockSize, capacity, comparator, combiner);
}

/**
 * @brief 为指定分区分配键值对缓冲区，内存不足触发溢写
 * @param partitionId 分区ID
 * @param kvlength 键值对总长度
 * @return 分配得到的缓冲区指针
 */
KVBuffer * MapOutputCollector::allocateKVBuffer(uint32_t partitionId, uint32_t kvlength) {
  PartitionBucket * partition = getPartition(partitionId);
  if (NULL == partition) {
    THROW_EXCEPTION_EX(IOException, "Partition is NULL, partition_id: %d, num_partitions: %d",
                       partitionId, _numPartitions);
  }

  KVBuffer * dest = partition->allocateKVBuffer(kvlength);

  if (NULL == dest) {
    string * spillpath = _spillOutput->getSpillPath();
    if (NULL == spillpath || spillpath->length() == 0) {
      THROW_EXCEPTION(IOException, "Illegal(empty) spill files path");
    } else {
      // 内存不足，触发中间溢写
      middleSpill(*spillpath, "", false);
      delete spillpath;
    }

    // 溢写后重新分配缓冲区
    dest = partition->allocateKVBuffer(kvlength);
    if (NULL == dest) {
      // io.sort.mb配置过小，无法容纳单个键值对
      THROW_EXCEPTION(OutOfMemoryException, "key/value pair larger than io.sort.mb");
    }
  }
  // 更新计数器
  _mapOutputRecords->increase();
  _mapOutputBytes->increase(kvlength - KVBuffer::headerLength());
  return dest;
}

/**
 * @brief 收集一个Map输出键值对
 * @param key 键地址
 * @param keylen 键长度
 * @param value 值地址
 * @param vallen 值长度
 * @param partitionId 分区ID
 * @return true 收集成功；false 缓冲区满需要溢写
 */
bool MapOutputCollector::collect(const void * key, uint32_t keylen, const void * value,
    uint32_t vallen, uint32_t partitionId) {
  uint32_t total_length = keylen + vallen + KVBuffer::headerLength();
  KVBuffer * buff = allocateKVBuffer(partitionId, total_length);

  if (NULL == buff) {
    return false;
  }
  buff->fill(key, keylen, value, vallen);
  return true;
}

/**
 * @brief 根据配置获取键比较器实例
 * @param config 配置对象
 * @param spec Map输出规格
 * @return 键比较器指针
 */
ComparatorPtr MapOutputCollector::getComparator(Config * config, MapOutputSpec & spec) {
  string nativeComparator = NATIVE_MAPOUT_KEY_COMPARATOR;
  const char * key_class = config->get(MAPRED_MAPOUTPUT_KEY_CLASS);
  if (NULL == key_class) {
    key_class = config->get(MAPRED_OUTPUT_KEY_CLASS);
  }
  nativeComparator.append(".").append(key_class);
  const char * comparatorName = config->get(nativeComparator);
  return NativeTask::get_comparator(spec.keyType, comparatorName);
}

/**
 * @brief 获取指定分区的存储桶
 * @param partition 分区ID
 * @return 分区存储桶指针，分区ID非法返回NULL
 */
PartitionBucket * MapOutputCollector::getPartition(uint32_t partition) {
  if (partition >= _numPartitions) {
    return NULL;
  }
  return _buckets[partition];
}

/**
 * @brief 对所有分区执行排序操作，统计排序耗时和记录数
 * @param orderType 排序顺序类型
 * @param sortType 排序算法类型
 * @param writer 输出写入器，为NULL时仅排序不输出
 * @param metric 输出排序统计指标
 */
void MapOutputCollector::sortPartitions(SortOrder orderType, SortAlgorithm sortType,
    IFileWriter * writer, SortMetrics & metric) {

  uint32_t start_partition = 0;
  uint32_t num_partition = _numPartitions;
  if (orderType == GROUPBY) {
    THROW_EXCEPTION(UnsupportException, "GROUPBY not supported");
  }

  uint64_t sortingTime = 0;
  Timer timer;
  uint64_t recordNum = 0;

  for (uint32_t i = 0; i < num_partition; i++) {
    if (NULL != writer) {
      writer->startPartition();
    }
    PartitionBucket * pb = _buckets[start_partition + i];
    if (pb != NULL) {
      recordNum += pb->getKVCount();
      if (orderType == FULLORDER) {
        timer.reset();
        pb->sort(sortType);
        sortingTime += timer.now() - timer.last();
      }
      if (NULL != writer) {
        pb->spill(writer);
      }
    }
    if (NULL != writer) {
      writer->endPartition();
    }
  }
  metric.sortTime = sortingTime;
  metric.recordCount = recordNum;
}

/**
 * @brief 执行中间溢写，将内存中已排序分区数据写入本地磁盘
 * @param spillOutput 溢写文件路径
 * @param indexFilePath 索引文件路径，为空则不输出索引
 * @param final 是否是最终溢写
 */
void MapOutputCollector::middleSpill(const std::string & spillOutput,
    const std::string & indexFilePath, bool final) {

  uint64_t collecttime = _collectTimer.now() - _collectTimer.last();

  if (spillOutput.empty()) {
    THROW_EXCEPTION(IOException, "MapOutputCollector: Spill file path empty");
  } else {
    // 创建本地输出流
    OutputStream * fout = FileSystem::getLocal().create(spillOutput, true);

    // 创建IFile格式写入器
    IFileWriter * writer = new IFileWriter(fout, _spec.checksumType, _spec.keyType, _spec.valueType,
        _spec.codec, _spilledRecords);

    Timer timer;
    SortMetrics metrics;
    sortPartitions(_spec.sortOrder, _spec.sortAlgorithm, writer, metrics);

    SingleSpillInfo * info = writer->getSpillInfo();
    info->path = spillOutput;
    uint64_t spillTime = timer.now() - timer.last() - metrics.sortTime;

    const uint64_t M = 1000000; // 转换单位为毫秒
    LOG("%s-spill: { id: %d, collect: %"PRIu64" ms, "
        "in-memory sort: %"PRIu64" ms, in-memory records: %"PRIu64", "
        "merge&spill: %"PRIu64" ms, uncompressed size: %"PRIu64", "
        "real size: %"PRIu64" path: %s }",
        final ? "Final" : "Mid",
        _spillInfos.getSpillCount(),
        collecttime / M,
        metrics.sortTime  / M,
        metrics.recordCount,
        spillTime  / M,
        info->getEndPosition(),
        info->getRealEndPosition(),
        spillOutput.c_str());

    if (final) {
      _mapOutputMaterializedBytes->increase(info->getRealEndPosition());
    }

    if (indexFilePath.length() > 0) {
      info->writeSpillInfo(indexFilePath);
      delete info;
    } else {
      // 添加到溢写信息列表，后续合并使用
      _spillInfos.add(info);
    }

    // 释放资源
    delete writer;
    delete fout;

    // 重置内存，准备收集新数据
    reset();
    _collectTimer.reset();
  }
}

/**
 * @brief 执行最终溢写，合并所有之前溢写文件和当前内存数据生成最终输出
 * @param filepath 最终输出文件路径
 * @param idx_file_path 最终输出索引文件路径
 */
void MapOutputCollector::finalSpill(const std::string & filepath,
    const std::string & idx_file_path) {

  // 无之前溢写，直接将当前内存数据溢写
  if (_spillInfos.getSpillCount() == 0) {
    middleSpill(filepath, idx_file_path, true);
    return;
  }

  // 创建最终输出写入器
  IFileWriter * writer = IFileWriter::create(filepath, _spec, _spilledRecords);
  Merger * merger = new Merger(writer, _config, _keyComparator, _combineRunner);

  // 将所有已有溢写文件添加为合并输入
  for (size_t i