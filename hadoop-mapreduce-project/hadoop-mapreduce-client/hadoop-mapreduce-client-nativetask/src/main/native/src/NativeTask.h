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
 * @file NativeTask.h
 * @brief Hadoop MapReduce 本地任务执行核心头文件，定义了本地任务运行所需的基础抽象类、枚举和宏
 *
 * 本文件属于MapReduce本地任务模块，提供C++实现的MapReduce任务运行时核心基础能力，
 * 支持Java端调用本地C++代码执行Map/Reduce任务，提升计算密集型任务的性能。
 */

#ifndef NATIVETASK_H_
#define NATIVETASK_H_

#include "lib/jniutils.h"
#include <stdint.h>
#include <string>
#include <vector>
#include <map>

namespace NativeTask {

using std::string;
using std::vector;
using std::map;
using std::pair;

/**
 * 本地对象类型枚举
 */
enum NativeObjectType {
  UnknownObjectType = 0,
  BatchHandlerType = 1,
};

/**
 * 字节序枚举
 */
enum Endium {
  LITTLE_ENDIUM = 0,
  LARGE_ENDIUM = 1
};

// 配置项名称定义
#define NATIVE_COMBINER "native.combiner.class"
#define NATIVE_PARTITIONER "native.partitioner.class"
#define NATIVE_MAPPER "native.mapper.class"
#define NATIVE_RECORDREADER "native.recordreader.class"
#define NATIVE_RECORDWRITER "native.recordwriter.class"

#define NATIVE_REDUCER "native.reducer.class"
#define NATIVE_HADOOP_VERSION "native.hadoop.version"

#define NATIVE_INPUT_SPLIT "native.input.split"
#define INPUT_LINE_KV_SEPERATOR "mapreduce.input.keyvaluelinerecordreader.key.value.separator"
#define MAPRED_TEXTOUTPUT_FORMAT_SEPERATOR "mapreduce.output.textoutputformat.separator"
#define MAPRED_WORK_OUT_DIR "mapreduce.task.output.dir"
#define MAPRED_COMPRESS_OUTPUT "mapreduce.output.fileoutputformat.compress"
#define MAPRED_OUTPUT_COMPRESSION_CODEC "mapreduce.output.fileoutputformat.compress.codec"
#define TOTAL_ORDER_PARTITIONER_PATH "total.order.partitioner.path"
#define TOTAL_ORDER_PARTITIONER_MAX_TRIE_DEPTH "total.order.partitioner.max.trie.depth"
#define FS_DEFAULT_NAME "fs.default.name"
#define FS_DEFAULT_FS "fs.defaultFS"

#define NATIVE_SORT_TYPE "native.sort.type"
#define MAPRED_SORT_AVOID "mapreduce.sort.avoidance"
#define NATIVE_SORT_MAX_BLOCK_SIZE "native.sort.blocksize.max"
#define MAPRED_COMPRESS_MAP_OUTPUT "mapreduce.map.output.compress"
#define MAPRED_MAP_OUTPUT_COMPRESSION_CODEC "mapreduce.map.output.compress.codec"
#define MAPRED_MAPOUTPUT_KEY_CLASS "mapreduce.map.output.key.class"
#define MAPRED_OUTPUT_KEY_CLASS "mapreduce.job.output.key.class"
#define MAPRED_MAPOUTPUT_VALUE_CLASS "mapreduce.map.output.value.class"
#define MAPRED_OUTPUT_VALUE_CLASS "mapreduce.job.output.value.class"
#define MAPRED_IO_SORT_MB "mapreduce.task.io.sort.mb"
#define MAPRED_NUM_REDUCES "mapreduce.job.reduces"
#define MAPRED_COMBINE_CLASS_OLD "mapred.combiner.class"
#define MAPRED_COMBINE_CLASS_NEW "mapreduce.job.combine.class"

#define NATIVE_LOG_DEVICE "native.log.device"

// 内置类库配置格式: name=path,name=path,name=path
#define NATIVE_CLASS_LIBRARY_BUILDIN "native.class.library.buildin"

#define NATIVE_MAPOUT_KEY_COMPARATOR "native.map.output.key.comparator"

/**
 * @brief 将本地对象类型转换为字符串表示
 * @param type 本地对象类型枚举值
 * @return 类型对应的字符串
 */
extern const std::string NativeObjectTypeToString(NativeObjectType type);

/**
 * @brief 从字符串解析本地对象类型
 * @param type 类型字符串
 * @return 解析后的枚举类型
 */
extern NativeObjectType NativeObjectTypeFromString(const std::string type);

/**
 * @class NativeObject
 * @brief 所有可动态加载本地对象的基类
 *
 * 所有能从动态共享库加载、由NativeObjectFactory管理的对象都需要继承此类，
 * 提供统一的类型识别和生命周期管理。
 */
class NativeObject {
public:
  virtual NativeObjectType type() {
    return UnknownObjectType;
  }

  virtual ~NativeObject() {
  }
  ;
};

/**
 * @brief 模板方法，创建指定类型的NativeObject实例
 * @tparam T 要创建的对象类型
 * @return 新建对象指针
 */
template<typename T>
NativeObject * ObjectCreator() {
  return new T();
}

typedef NativeObject * (*ObjectCreatorFunc)();

typedef ObjectCreatorFunc (*GetObjectCreatorFunc)(const std::string & name);

typedef void * (*FunctionGetter)(const std::string & name);

typedef int32_t (*InitLibraryFunc)();

/**
 * @class HadoopException
 * @brief Hadoop本地任务异常基类，继承自标准C++异常
 */
class HadoopException : public std::exception {
private:
  std::string _reason;
public:
  HadoopException(const string & what);
  virtual ~HadoopException() throw () {
  }

  virtual const char* what() const throw () {
    return _reason.c_str();
  }
};

/**
 * @class OutOfMemoryException
 * @brief 内存不足异常
 */
class OutOfMemoryException : public HadoopException {
public:
  OutOfMemoryException(const string & what)
      : HadoopException(what) {
  }
};

/**
 * @class IOException
 * @brief IO操作异常
 */
class IOException : public HadoopException {
public:
  IOException(const string & what)
      : HadoopException(what) {
  }
};

/**
 * @class UnsupportException
 * @brief 不支持操作异常
 */
class UnsupportException : public HadoopException {
public:
  UnsupportException(const string & what)
      : HadoopException(what) {
  }
};

/**
 * @class JavaException
 * @brief 通过JNI调用Java方法时抛出的异常
 */
class JavaException : public HadoopException {
public:
  JavaException(const string & what)
      : HadoopException(what) {
  }
};

// 辅助宏，用于生成带文件行号的异常信息
#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define AT __FILE__ ":" TOSTRING(__LINE__)
#define THROW_EXCEPTION(type, what) throw type((std::string(AT":") + what))
#define THROW_EXCEPTION_EX(type, fmt, args...) \
        throw type(StringUtil::Format("%s:" fmt, AT, ##args))

/**
 * @class Config
 * @brief 配置存储与读取类，保存MapReduce任务的配置参数
 */
class Config {
protected:
  map<string, string> _configs;
public:
  Config() {
  }
  ~Config() {
  }

  /**
   * @brief 根据配置键获取配置值
   * @param name 配置键
   * @return 配置值指针，不存在返回NULL
   */
  const char * get(const string & name);

  /**
   * @brief 根据配置键获取配置值，不存在返回默认值
   * @param name 配置键
   * @param defaultValue 默认值
   * @return 配置值字符串
   */
  string get(const string & name, const string & defaultValue);

  /**
   * @brief 获取布尔类型配置值
   * @param name 配置键
   * @param defaultValue 默认值
   * @return 布尔配置值
   */
  bool getBool(const string & name, bool defaultValue);

  /**
   * @brief 获取整数类型配置值
   * @param name 配置键
   * @param defaultValue 默认值，默认为-1
   * @return 整数配置值
   */
  int64_t getInt(const string & name, int64_t defaultValue = -1);

  /**
   * @brief 获取浮点类型配置值
   * @param name 配置键
   * @param defaultValue 默认值，默认为-1
   * @return 浮点配置值
   */
  float getFloat(const string & name, float defaultValue = -1);

  /**
   * @brief 获取逗号分隔的字符串列表配置
   * @param name 配置键
   * @param dest 输出结果向量
   */
  void getStrings(const string & name, vector<string> & dest);

  /**
   * @brief 获取逗号分隔的整数列表配置
   * @param name 配置键
   * @param dest 输出结果向量
   */
  void getInts(const string & name, vector<int64_t> & dest);

  /**
   * @brief 获取逗号分隔的浮点数列表配置
   * @param name 配置键
   * @param dest 输出结果向量
   */
  void getFloats(const string & name, vector<float> & dest);

  /**
   * @brief 设置字符串配置项
   * @param key 配置键
   * @param value 配置值
   */
  void set(const string & key, const string & value);

  /**
   * @brief 设置整数配置项
   * @param name 配置键
   * @param value 配置值
   */
  void setInt(const string & name, int64_t value);

  /**
   * @brief 设置布尔配置项
   * @param name 配置键
   * @param value 配置值
   */
  void setBool(const string & name, bool value);

  /**
   * 从配置文件加载配置，格式为：
   * # comment
   * key1=value1
   * key2=value2
   * ...
   * @param path 配置文件路径
   */
  void load(const string & path);

  /**
   * 从命令行参数解析配置，格式为：
   * key1=value1 key2=value2,value2
   * @param argc 参数个数
   * @param argv 参数数组
   */
  void parse(int32_t argc, const char ** argv);
};

/**
 * @class Command
 * @brief 命令标识类，封装命令ID和描述信息
 */
class Command {
private:
  int _id;
  const char * _description;

public:
  Command(int id, const char * description)
      : _id(id), _description(description) {
  }

  Command(int id)
      : _id(id), _description(NULL) {
  }

  int id() const {
    return _id;
  }

  const char * description() const {
    return _description;
  }

  bool equals(const Command & other) const {
    if (_id == other._id) {
      return true;
    }
    return false;
  }
};

/**
 * @class Buffer
 * @brief 字节缓冲区封装，存储数据指针和长度，不管理内存
 */
class Buffer {
protected:
  const char * _data;
  uint32_t _length;

public:
  Buffer()
      : _data(NULL), _length(0) {
  }

  Buffer(const char * data, uint32_t length)
      : _data(data), _length(length) {
  }

  ~Buffer() {
  }

  void reset(const char * data, uint32_t length) {
    this->_data = data;
    this->_length = length;
  }

  const char * data() const {
    return _data;
  }

  uint32_t length() const {
    return _length;
  }

  void data(const char * data) {
    this->_data = data;
  }

  void length(uint32_t length) {
    this->_length = length;
  }

  string toString() const {
    return string(_data, _length);
  }
};

/**
 * @class InputSplit
 * @brief 输入分片抽象接口，定义Map任务输入数据分片的行为
 */
class InputSplit {
public:
  virtual uint64_t getLength() = 0;
  virtual vector<string> & getLocations() = 0;
  virtual void readFields(const string & data) = 0;
  virtual void writeFields(string & dest) = 0;
  virtual string toString() = 0;

  virtual ~InputSplit() {

  }
};

/**
 * @class Configurable
 * @brief 可配置对象抽象基类，支持在对象创建后注入配置
 */
class Configurable : public NativeObject {
public:
  Configurable() {
  }

  virtual void configure(Config * config) {
  }
};

/**
 * @class Collector
 * @brief 键值对输出收集器抽象接口，供Map/Combine任务输出结果
 */
class Collector {
public:
  virtual ~Collector() {
  }

  virtual void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen) {
  }

  virtual void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen,
      int32_t partition) {
    collect(key, keyLen, value, valueLen);
  }
};

/**
 * @class Progress
 * @brief 进度查询抽象接口，用于获取任务执行进度
 */
class Progress {
public:
  virtual ~Progress() {
  }
  virtual float getProgress() = 0;
};

/**
 * @class Counter
 * @brief 计数器实现，用于统计任务执行指标，可同步回Java端
 */
class Counter {
private:
  // 非线程安全，TODO：需要改为原子操作
  volatile uint64_t _count;

  string _group;
  string _name;
public:
  Counter(const string & group, const string & name)
      : _count(0), _group(group), _name(name) {
  }

  const string & group() const {
    return _group;
  }
  const string & name() const {
    return _name;
  }

  uint64_t get() const {
    return _count;
  }

  void increase() {
    _count++;
  }

  void increase(uint64_t cnt) {
    _count += cnt;
  }
};

/**
 * @class KVIterator
 * @brief 键值对迭代器抽象接口，用于遍历排序后的键值对
 */
class KVIterator {
public:
  virtual ~KVIterator() {
  }
  virtual bool next(Buffer & key, Buffer & value) = 0;
};

/**
 * @class ProcessorBase
 * @brief 处理基类，所有Map/Reduce/Combine处理器的基类，持有输出收集器
 */
class ProcessorBase : public Configurable {
protected:
  Collector * _collector;
public:
  ProcessorBase()
      : _collector(NULL) {
  }

  void setCollector(Collector * collector) {
    _collector = collector;
  }

  Collector * getCollector() {
    return _collector;
  }

  void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen) {
    _collector->collect(key, keyLen, value, valueLen);
  }

  void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen,
      int32_t partition) {
    _collector->collect(key, keyLen, value, valueLen, partition);
  }

  Counter * getCounter(const string & group, const string & name);

  virtual void close() {
  }
};

/**
 * 键分组迭代状态枚举
 */
enum KeyGroupIterState {
  SAME_KEY,
  NEW_KEY,
  NEW_KEY_VALUE,
  NO_MORE,
};

/**
 * @class KeyGroupIterator
 * @brief 按键分组迭代器抽象接口，供Reduce阶段按相同key分组迭代值
 */
class KeyGroupIterator {
public:
  virtual ~KeyGroupIterator() {
  }
  /**
   * 移动到下一个key分组，或初始化迭代器
   * @return 是否还有下一个key分组
   */
  virtual bool nextKey() = 0;

  /**
   * 获取当前分组的key
   * @param len 输出key长度
   * @return key数据指针
   */
  virtual const char * getKey(uint32_t & len) = 0;

  /**
   * 获取当前key分组的下一个值
   * @param len 输出值长度
   * @return 值数据指针，无更多值返回NULL
   */
  virtual