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
 * @file NativeObjectFactory.cc
 * @brief MapReduce本地任务 原生对象工厂实现，负责原生对象创建、动态库加载和比较器管理
 * 
 * 该文件是Hadoop MapReduce本地任务模块的核心工厂类，提供：
 * 1. 原生对象的动态创建与生命周期管理
 * 2. 支持加载额外用户自定义动态库扩展功能
 * 3. 统一管理任务进度、状态和度量计数器
 * 4. 提供不同键类型的默认比较器实现
 */

#include <signal.h>
#ifndef __CYGWIN__
#include <execinfo.h>
#endif
#include "lib/commons.h"
#include "NativeTask.h"
#include "lib/NativeObjectFactory.h"
#include "lib/NativeLibrary.h"
#include "lib/BufferStream.h"
#include "util/StringUtil.h"
#include "util/SyncUtils.h"
#include "util/WritableUtils.h"
#include "handler/BatchHandler.h"
#include "handler/MCollectorOutputHandler.h"
#include "handler/CombineHandler.h"

using namespace NativeTask;

// TODO: just for debug, should be removed
/**
 * @brief 信号处理函数，用于调试时打印调用栈
 * @param sig 触发的信号编号
 */
extern "C" void handler(int sig) {
  void *array[10];
  size_t size;

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);

#ifndef __CYGWIN__
  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  backtrace_symbols_fd(array, size, 2);
#endif

  exit(1);
}

/**
 * @brief 原生任务库注册宏，注册内置处理器类并设置默认类型映射
 */
DEFINE_NATIVE_LIBRARY(NativeTask) {
  REGISTER_CLASS(BatchHandler, NativeTask);
  REGISTER_CLASS(CombineHandler, NativeTask);
  REGISTER_CLASS(MCollectorOutputHandler, NativeTask);
  NativeObjectFactory::SetDefaultClass(BatchHandlerType, "NativeTask.BatchHandler");
}

/**
 * @brief MapReduce本地任务命名空间
 */
namespace NativeTask {

static Config G_CONFIG;

vector<NativeLibrary *> NativeObjectFactory::Libraries;
map<NativeObjectType, string> NativeObjectFactory::DefaultClasses;
Config * NativeObjectFactory::GlobalConfig = &G_CONFIG;
float NativeObjectFactory::LastProgress = 0;
Progress * NativeObjectFactory::TaskProgress = NULL;
string NativeObjectFactory::LastStatus;
set<Counter *, CounterPtrCompare> NativeObjectFactory::CounterSet;
vector<Counter *> NativeObjectFactory::Counters;
vector<uint64_t> NativeObjectFactory::CounterLastUpdateValues;
bool NativeObjectFactory::Inited = false;

static Lock FactoryLock;

/**
 * @brief 初始化原生对象工厂，加载内置和用户自定义动态库
 * @return 初始化成功返回true，失败返回false
 */
bool NativeObjectFactory::Init() {
  ScopeLock<Lock> autolocak(FactoryLock);
  if (Inited == false) {
    // 根据配置设置日志输出设备
    string device = GetConfig().get(NATIVE_LOG_DEVICE, "stderr");
    if (device == "stdout") {
      LOG_DEVICE = stdout;
    } else if (device == "stderr") {
      LOG_DEVICE = stderr;
    } else {
      LOG_DEVICE = fopen(device.c_str(), "w");
    }
    NativeTaskInit();
    // 加载内置原生任务库
    NativeLibrary * library = new NativeLibrary("libnativetask.so", "NativeTask");
    library->_getObjectCreatorFunc = NativeTaskGetObjectCreator;
    Libraries.push_back(library);
    Inited = true;
    // 加载用户配置的额外扩展动态库
    string libraryConf = GetConfig().get(NATIVE_CLASS_LIBRARY_BUILDIN, "");
    if (libraryConf.length() > 0) {
      vector<string> libraries;
      vector<string> pair;
      StringUtil::Split(libraryConf, ",", libraries, true);
      for (size_t i = 0; i < libraries.size(); i++) {
        pair.clear();
        StringUtil::Split(libraries[i], "=", pair, true);
        if (pair.size() == 2) {
          string & name = pair[0];
          string & path = pair[1];
          LOG("[NativeObjectLibrary] Try to load library [%s] with file [%s]", name.c_str(),
              path.c_str());
          if (false == RegisterLibrary(path, name)) {
            LOG("[NativeObjectLibrary] RegisterLibrary failed: name=%s path=%s", name.c_str(),
                path.c_str());
            return false;
          } else {
            LOG("[NativeObjectLibrary] RegisterLibrary success: name=%s path=%s", name.c_str(),
                path.c_str());
          }
        } else {
          LOG("[NativeObjectLibrary] Illegal native.class.libray: [%s] in [%s]",
              libraries[i].c_str(), libraryConf.c_str());
        }
      }
    }
    const char * version = GetConfig().get(NATIVE_HADOOP_VERSION);
    LOG("[NativeObjectLibrary] NativeTask library initialized with hadoop %s",
        version == NULL ? "unkown" : version);
  }
  return true;
}

/**
 * @brief 释放工厂所有资源，清理已加载的库和计数器
 */
void NativeObjectFactory::Release() {
  ScopeLock<Lock> autolocak(FactoryLock);
  // 逆序释放所有动态库
  for (ssize_t i = Libraries.size() - 1; i >= 0; i--) {
    delete Libraries[i];
    Libraries[i] = NULL;
  }
  Libraries.clear();
  // 释放所有计数器
  for (size_t i = 0; i < Counters.size(); i++) {
    delete Counters[i];
  }
  Counters.clear();
  // 关闭自定义日志文件
  if (LOG_DEVICE != stdout && LOG_DEVICE != stderr) {
    fclose(LOG_DEVICE);
    LOG_DEVICE = stderr;
  }
  Inited = false;
}

/**
 * @brief 检查工厂是否已初始化，未初始化则执行初始化
 * @throw IOException 如果初始化失败抛出IO异常
 */
void NativeObjectFactory::CheckInit() {
  if (Inited == false) {
    if (!Init()) {
      throw new IOException("Init NativeTask library failed.");
    }
  }
}

/**
 * @brief 获取全局配置对象引用
 * @return 全局配置对象引用
 */
Config & NativeObjectFactory::GetConfig() {
  return *GlobalConfig;
}

/**
 * @brief 获取全局配置对象指针
 * @return 全局配置对象指针
 */
Config * NativeObjectFactory::GetConfigPtr() {
  return GlobalConfig;
}

/**
 * @brief 设置任务进度数据源
 * @param progress 进度对象指针
 */
void NativeObjectFactory::SetTaskProgressSource(Progress * progress) {
  TaskProgress = progress;
}

/**
 * @brief 获取当前任务进度
 * @return 进度值，0.0-1.0
 */
float NativeObjectFactory::GetTaskProgress() {
  if (TaskProgress != NULL) {
    LastProgress = TaskProgress->getProgress();
  }
  return LastProgress;
}

/**
 * @brief 设置当前任务状态文本
 * @param status 任务状态文本
 */
void NativeObjectFactory::SetTaskStatus(const string & status) {
  LastStatus = status;
}

static Lock CountersLock;

/**
 * @brief 获取任务进度、状态和计数器增量更新，序列化后返回给Java端
 * @param statusData 输出序列化后的状态数据缓冲区
 */
void NativeObjectFactory::GetTaskStatusUpdate(string & statusData) {
  // Encoding:
  // progress:float
  // status:Text
  // Counter number
  // Counters[group:Text, name:Text, incrCount:Long]
  OutputStringStream os(statusData);
  float progress = GetTaskProgress();
  WritableUtils::WriteFloat(&os, progress);
  WritableUtils::WriteText(&os, LastStatus);
  LastStatus.clear();
  {
    ScopeLock<Lock> AutoLock(CountersLock);
    uint32_t numCounter = (uint32_t)Counters.size();
    WritableUtils::WriteInt(&os, numCounter);
    // 逐个写入计数器增量变化
    for (size_t i = 0; i < numCounter; i++) {
      Counter * counter = Counters[i];
      uint64_t newCount = counter->get();
      uint64_t incr = newCount - CounterLastUpdateValues[i];
      CounterLastUpdateValues[i] = newCount;
      WritableUtils::WriteText(&os, counter->group());
      WritableUtils::WriteText(&os, counter->name());
      WritableUtils::WriteLong(&os, incr);
    }
  }
}

/**
 * @brief 获取指定分组和名称的计数器，不存在则创建新计数器
 * @param group 计数器分组
 * @param name 计数器名称
 * @return 计数器对象指针
 */
Counter * NativeObjectFactory::GetCounter(const string & group, const string & name) {
  ScopeLock<Lock> AutoLock(CountersLock);
  Counter tmpCounter(group, name);
  set<Counter *>::iterator itr = CounterSet.find(&tmpCounter);
  if (itr != CounterSet.end()) {
    return *itr;
  }
  Counter * ret = new Counter(group, name);
  Counters.push_back(ret);
  CounterLastUpdateValues.push_back(0);
  CounterSet.insert(ret);
  return ret;
}

/**
 * @brief 注册原生对象创建函数到工厂
 * @param clz 原生对象类名
 * @param func 对象创建函数指针
 */
void NativeObjectFactory::RegisterClass(const string & clz, ObjectCreatorFunc func) {
  NativeTaskClassMap__[clz] = func;
}

/**
 * @brief 根据类名创建原生对象
 * @param clz 原生对象类名
 * @return 创建的原生对象指针，创建失败返回NULL
 */
NativeObject * NativeObjectFactory::CreateObject(const string & clz) {
  ObjectCreatorFunc creator = GetObjectCreator(clz);
  return creator ? creator() : NULL;
}

/**
 * @brief 根据函数名从已加载的动态库中查找函数指针
 * @param funcName 函数名称
 * @return 函数指针，找不到返回NULL
 */
void * NativeObjectFactory::GetFunction(const string & funcName) {
  CheckInit();
  {
    // 逆序查找，后加载的库优先
    for (vector<NativeLibrary*>::reverse_iterator ritr = Libraries.rbegin();
        ritr != Libraries.rend(); ritr++) {
      void * ret = (*ritr)->getFunction(funcName);
      if (NULL != ret) {
        return ret;
      }
    }
    return NULL;
  }
}

/**
 * @brief 根据类名查找对象创建函数
 * @param clz 原生对象类名
 * @return 对象创建函数指针，找不到返回NULL
 */
ObjectCreatorFunc NativeObjectFactory::GetObjectCreator(const string & clz) {
  CheckInit();
  {
    // 逆序查找，后加载的库优先
    for (vector<NativeLibrary*>::reverse_iterator ritr = Libraries.rbegin();
        ritr != Libraries.rend(); ritr++) {
      ObjectCreatorFunc ret = (*ritr)->getObjectCreator(clz);
      if (NULL != ret) {
        return ret;
      }
    }
    return NULL;
  }
}

/**
 * @brief 释放原生对象
 * @param obj 要释放的原生对象指针
 */
void NativeObjectFactory::ReleaseObject(NativeObject * obj) {
  delete obj;
}

/**
 * @brief 注册加载外部动态库到工厂
 * @param path 动态库文件路径
 * @param name 库名称
 * @return 注册成功返回true，失败返回false
 */
bool NativeObjectFactory::RegisterLibrary(const string & path, const string & name) {
  CheckInit();
  {
    NativeLibrary * library = new NativeLibrary(path, name);
    bool ret = library->init();
    if (!ret) {
      delete library;
      return false;
    }
    Libraries.push_back(library);
    return true;
  }
}

static Lock DefaultClassesLock;

/**
 * @brief 设置指定对象类型的默认实现类名
 * @param type 对象类型枚举
 * @param clz 默认实现类全名
 */
void NativeObjectFactory::SetDefaultClass(NativeObjectType type, const string & clz) {
  ScopeLock<Lock> autolocak(DefaultClassesLock);
  DefaultClasses[type] = clz;
}

/**
 * @brief 根据对象类型创建默认实现对象
 * @param type 对象类型枚举
 * @return 创建的原生对象指针，未找到默认类返回NULL
 */
NativeObject * NativeObjectFactory::CreateDefaultObject(NativeObjectType type) {
  CheckInit();
  {
    if (DefaultClasses.find(type) != DefaultClasses.end()) {
      string clz = DefaultClasses[type];
      return CreateObject(clz);
    }
    LOG("[NativeObjectLibrary] Default class for NativeObjectType %s not found",
        NativeObjectTypeToString(type).c_str());
    return NULL;
  }
}

/**
 * @brief 字节数组比较器，按字典序比较二进制键
 * @param src 第一个键二进制数据
 * @param srcLength 第一个键长度
 * @param dest 第二个键二进制数据
 * @param destLength 第二个键长度
 * @return 比较结果：src大返回1，dest大返回-1，相等返回0
 */
int NativeObjectFactory::BytesComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {

  uint32_t minlen = std::min(srcLength, destLength);
  int64_t ret = fmemcmp(src, dest, minlen);
  if (ret > 0) {
    return 1;
  } else if (ret < 0) {
    return -1;
  }
  return srcLength - destLength;
}

/**
 * @brief 单字节比较器
 * @param src 第一个键二进制数据
 * @param srcLength 第一个键长度
 * @param dest 第二个键二进制数据
 * @param destLength 第二个键长度
 * @return 比较结果：src大返回1，dest大返回-1，相等返回0
 */
int NativeObjectFactory::ByteComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  return (*src) - (*dest);
}

/**
 * @brief 32位整数比较器，处理大端字节序
 * @param src 第一个键二进制数据
 * @param srcLength 第一个键长度
 * @param dest 第二个键二进制数据
 * @param destLength 第二个键长度
 * @return 比较结果：src大返回1，dest大返回-1，相等返回0
 */
int NativeObjectFactory::IntComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int result = (*src) - (*dest);
  if (result == 0) {
    uint32_t from = bswap(*(uint32_t*)src);
    uint32_t to = bswap(*(uint32_t*)dest);
    if (from > to) {
      return 1;
    } else if (from == to) {
      return 0;
    } else {
      return -1;
    }
  }
  return result;
}

/**
 * @brief 64位长整数比较器，处理大端字节序
 * @param src 第一个键二进制数据
 * @param srcLength 第一个键长度
 * @param dest 第二个键二进制数据
 * @param destLength 第二个键长度
 * @return 比较结果：src大返回1，dest大返回-1，相等返回0
 */
int NativeObjectFactory::LongComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int result = (int)(*src) - (int)(*dest);
  if (result == 0) {

    uint64_t from = bswap64(*(uint64_t*)src);
    uint64_t to = bswap64(*(uint64_t*)dest);
    if (from > to) {
      return 1;
    } else if (from == to) {
      return 0