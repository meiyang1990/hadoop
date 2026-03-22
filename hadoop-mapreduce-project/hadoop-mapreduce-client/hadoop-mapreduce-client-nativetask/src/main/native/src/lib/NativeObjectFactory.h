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
 * @file NativeObjectFactory.h
 * @brief Hadoop MapReduce原生任务对象工厂头文件，定义原生对象创建与管理能力
 * 
 * 属于MapReduce原生任务模块，负责动态创建、管理原生C++对象，支持动态加载原生库扩展功能
 */

#ifndef NATIVEOBJECTFACTORY_H_
#define NATIVEOBJECTFACTORY_H_

#include <string>
#include <vector>
#include <set>
#include <map>

#include "NativeTask.h"

namespace NativeTask {

using std::string;
using std::vector;
using std::map;
using std::set;
using std::pair;

class NativeLibrary;

/**
 * @brief 计数器指针比较器，用于对计数器按分组和名称排序
 * 
 * 支持CounterSet集合中按分组(group)和名称(name)对计数器指针去重排序
 */
class CounterPtrCompare {
public:
  bool operator()(const Counter * lhs, const Counter * rhs) const {
    if (lhs->group() < rhs->group()) {
      return true;
    } else if (lhs->group() == rhs->group()) {
      return lhs->name() < rhs->name();
    } else {
      return false;
    }
  }
};

/**
 * @class NativeObjectFactory
 * @brief 原生对象工厂类，负责MapReduce原生任务中所有原生对象的创建、生命周期管理
 * 
 * 核心职责：
 * 1. 初始化与管理全局配置、任务进度、计数器等全局状态
 * 2. 支持动态注册对象创建函数，动态加载第三方原生库
 * 3. 根据名称或类型创建对应原生对象，统一管理对象生命周期
 * 4. 提供不同数据类型的二进制比较器实现，用于MapReduce排序阶段
 * 
 * 所有方法均为静态方法，整个任务共享一个工厂实例
 */
class NativeObjectFactory {
private:
  static vector<NativeLibrary *> Libraries;               // 已加载的原生动态库列表
  static map<NativeObjectType, string> DefaultClasses;    // 各类对象的默认实现类名称映射
  static Config * GlobalConfig;                            // 全局配置对象指针
  static float LastProgress;                               // 上次上报的任务进度值
  static Progress * TaskProgress;                          // 任务进度源对象指针
  static string LastStatus;                                // 上次上报的任务状态文本
  static set<Counter *, CounterPtrCompare> CounterSet;     // 去重排序后的计数器集合
  static vector<Counter *> Counters;                       // 计数器列表，用于顺序遍历
  static vector<uint64_t> CounterLastUpdateValues;         // 计数器上次上报的值，用于增量更新
  static bool Inited;                                       // 工厂是否已初始化标记
public:
  /**
   * @brief 初始化原生对象工厂，注册默认对象类型
   * @return 初始化成功返回true，失败返回false
   */
  static bool Init();
  /**
   * @brief 释放工厂所有资源，卸载已加载的动态库，销毁所有对象
   */
  static void Release();
  /**
   * @brief 检查工厂是否已初始化，未初始化则抛出异常
   */
  static void CheckInit();
  /**
   * @brief 获取全局配置对象引用
   * @return 全局配置对象引用
   */
  static Config & GetConfig();
  /**
   * @brief 获取全局配置对象指针
   * @return 全局配置对象指针
   */
  static Config * GetConfigPtr();
  /**
   * @brief 设置任务进度源，用于获取当前任务进度
   * @param progress 进度对象指针
   */
  static void SetTaskProgressSource(Progress * progress);
  /**
   * @brief 获取当前任务进度值
   * @return 当前任务进度(0.0~1.0)
   */
  static float GetTaskProgress();
  /**
   * @brief 设置当前任务状态文本
   * @param status 任务状态文本
   */
  static void SetTaskStatus(const string & status);
  /**
   * @brief 获取任务状态更新，将状态变更序列化到输出缓冲区
   * @param statusData 输出状态数据
   */
  static void GetTaskStatusUpdate(string & statusData);
  /**
   * @brief 获取指定分组和名称的计数器，不存在则创建
   * @param group 计数器分组名称
   * @param name 计数器名称
   * @return 计数器对象指针
   */
  static Counter * GetCounter(const string & group, const string & name);
  /**
   * @brief 注册对象类型与其创建函数的映射
   * @param clz 对象类名
   * @param func 对象创建函数指针
   */
  static void RegisterClass(const string & clz, ObjectCreatorFunc func);
  /**
   * @brief 根据类名创建对应原生对象
   * @param clz 要创建的对象类名
   * @return 创建好的原生对象指针
   */
  static NativeObject * CreateObject(const string & clz);
  /**
   * @brief 从已加载库中获取指定函数地址
   * @param clz 函数名称
   * @return 函数指针，未找到返回NULL
   */
  static void * GetFunction(const string & clz);
  /**
   * @brief 获取指定类名对应的对象创建函数
   * @param clz 对象类名
   * @return 对象创建函数指针
   */
  static ObjectCreatorFunc GetObjectCreator(const string & clz);
  /**
   * @brief 释放原生对象资源
   * @param obj 要释放的对象指针
   */
  static void ReleaseObject(NativeObject * obj);
  /**
   * @brief 加载并注册指定路径的动态库
   * @param path 动态库文件路径
   * @param name 库名称
   * @return 加载成功返回true，失败返回false
   */
  static bool RegisterLibrary(const string & path, const string & name);
  /**
   * @brief 设置指定对象类型的默认实现类
   * @param type 对象类型枚举
   * @param clz 默认实现类名
   */
  static void SetDefaultClass(NativeObjectType type, const string & clz);
  /**
   * @brief 创建指定类型的默认对象
   * @param type 对象类型枚举
   * @return 创建好的原生对象指针
   */
  static NativeObject * CreateDefaultObject(NativeObjectType type);
  /**
   * @brief 字节数组二进制比较器，按字典序比较
   * @param src 第一个字节数组地址
   * @param srcLength 第一个字节数组长度
   * @param dest 第二个字节数组地址
   * @param destLength 第二个字节数组长度
   * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
   */
  static int BytesComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  /**
   * @brief 单字节比较器
   * @param src 第一个字节地址
   * @param srcLength 第一个字节长度（必须为1）
   * @param dest 第二个字节地址
   * @param destLength 第二个字节长度（必须为1）
   * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
   */
  static int ByteComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  /**
   * @brief 定长整数比较器（大端字节序）
   * @param src 第一个整数字节地址
   * @param srcLength 第一个整数字节长度（必须为4）
   * @param dest 第二个整数字节地址
   * @param destLength 第二个整数字节长度（必须为4）
   * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
   */
  static int IntComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  /**
   * @brief 定长长整数比较器（大端字节序）
   * @param src 第一个长整数字节地址
   * @param srcLength 第一个长整数字节长度（必须为8）
   * @param dest 第二个长整数字节地址
   * @param destLength 第二个长整数字节长度（必须为8）
   * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
   */
  static int LongComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  /**
   * @brief 可变长度整数（VInt）比较器
   * @param src 第一个VInt字节地址
   * @param srcLength 第一个VInt字节长度
   * @param dest 第二个VInt字节地址
   * @param destLength 第二个VInt字节长度
   * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
   */
  static int VIntComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  /**
   * @brief 可变长度长整数（VLong）比较器
   * @param src 第一个VLong字节地址
   * @param srcLength 第一个VLong字节长度
   * @param dest 第二个VLong字节地址
   * @param destLength 第二个VLong字节长度
   * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
   */
  static int VLongComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  /**
   * @brief float类型比较器（大端字节序）
   * @param src 第一个float字节地址
   * @param srcLength 第一个float字节长度（必须为4）
   * @param dest 第二个float字节地址
   * @param destLength 第二个float字节长度（必须为4）
   * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
   */
  static int FloatComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  /**
   * @brief double类型比较器（大端字节序）
   * @param src 第一个double字节地址
   * @param srcLength 第一个double字节长度（必须为8）
   * @param dest 第二个double字节地址
   * @param destLength 第二个double字节长度（必须为8）
   * @return 比较结果：小于0表示src < dest，等于0表示相等，大于0表示src > dest
   */
  static int DoubleComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
};

} // namespace NativeTask

#endif /* NATIVEOBJECTFACTORY_H_ */