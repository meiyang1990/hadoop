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
 * 本地任务Combiner头文件，定义MapReduce本地Combiner相关抽象接口和上下文类
 * 属于Hadoop MapReduce本地任务模块，提供原生实现的Combiner运行框架
 */
#ifndef COMBINER_H_
#define COMBINER_H_
#include "commons.h"
#include "lib/IFile.h"

namespace NativeTask {

/**
 * 内存缓存键值对迭代器抽象接口，提供访问内存中存储的键值对数据能力
 * 继承自KVIterator，扩展获取内存基地址和偏移量的方法
 */
class MemoryBufferKVIterator : public KVIterator {
public:
  /**
   * 获取内存缓存基地址
   * @return 内存缓存起始指针
   */
  virtual const char * getBase() = 0;
  /**
   * 获取所有键值对偏移量数组指针
   * @return 存储键值对偏移量的vector指针
   */
  virtual std::vector<uint32_t> * getKVOffsets() = 0;
};

/**
 * 合并上下文类型枚举，定义不同的Combiner输入数据存储类型
 */
enum CombineContextType {
  UNKNOWN = 0,                      // 未知类型
  CONTINUOUS_MEMORY_BUFFER = 1,     // 连续内存缓存类型
};

/**
 * Combiner合并上下文基类，封装Combiner运行所需的上下文信息
 */
class CombineContext {

private:
  CombineContextType _type;  // 上下文类型

public:
  /**
   * 构造函数，初始化上下文类型
   * @param type 上下文类型
   */
  CombineContext(CombineContextType type)
      : _type(type) {
  }

public:
  /**
   * 获取当前上下文类型
   * @return 上下文类型枚举值
   */
  CombineContextType getType() {
    return _type;
  }
};

/**
 * 内存中Combiner合并上下文类，对应连续内存缓存的输入数据类型
 */
class CombineInMemory : public CombineContext {
  /**
   * 构造函数，设置上下文类型为连续内存缓存
   */
  CombineInMemory()
      : CombineContext(CONTINUOUS_MEMORY_BUFFER) {
  }
};

/**
 * Combiner运行器抽象接口，定义Combiner执行的统一入口
 */
class ICombineRunner {
public:
  /**
   * 构造函数
   */
  ICombineRunner() {
  }

  /**
   * 执行Combiner合并操作
   * @param context 合并上下文，包含输入数据类型信息
   * @param kvIterator 待合并键值对迭代器
   * @param writer 合并结果输出写入器
   */
  virtual void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer) = 0;

  /**
   * 析构函数
   */
  virtual ~ICombineRunner() {
  }
};

} /* namespace NativeTask */
#endif /* COMBINER_H_ */