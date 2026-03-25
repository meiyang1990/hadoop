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
 * @file NativeLibrary.h
 * @brief Hadoop MapReduce原生任务：动态本地库加载与管理头文件
 * 
 * 该文件定义了动态加载原生共享库的抽象接口，负责加载外部原生库，
 * 并从库中创建原生对象、获取函数，支持原生任务的动态扩展。
 */

#ifndef NATIVELIBRARY_H_
#define NATIVELIBRARY_H_

#include <string>

namespace NativeTask {

using std::string;
class NativeObject;
class NativeObjectFactory;

/**
 * @class NativeLibrary
 * @brief 原生动态共享库封装类
 * 
 * 负责加载指定路径的动态共享库，管理库的生命周期，
 * 提供从库中创建对象、获取函数的能力，是原生对象工厂
 * 动态加载扩展原生库的核心抽象。
 */
class NativeLibrary {
  friend class NativeObjectFactory;
private:
  string _path;
  string _name;
  GetObjectCreatorFunc _getObjectCreatorFunc;
  FunctionGetter _functionGetter;
public:
  /**
   * @brief 构造NativeLibrary实例
   * @param path 动态库文件路径
   * @param name 库名称
   */
  NativeLibrary(const string & path, const string & name);

  /**
   * @brief 初始化动态库，完成加载操作
   * @return 初始化成功返回true，失败返回false
   */
  bool init();

  /**
   * @brief 根据类名从动态库创建对应原生对象
   * @param clz 要创建的对象类名
   * @return 创建好的原生对象指针
   */
  NativeObject * createObject(const string & clz);

  /**
   * @brief 根据函数名从动态库获取函数指针
   * @param functionName 要获取的函数名称
   * @return 函数指针，获取失败返回NULL
   */
  void * getFunction(const string & functionName);

  /**
   * @brief 根据类名获取对象创建函数
   * @param clz 目标对象类名
   * @return 对象创建函数指针
   */
  ObjectCreatorFunc getObjectCreator(const string & clz);

  ~NativeLibrary() {
  }
};

} // namespace NativeTask

#endif /* NATIVELIBRARY_H_ */