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
 * @file NativeLibrary.cc
 * Hadoop MapReduce原生任务动态链接库管理实现，负责加载第三方原生动态库并提供对象创建和函数获取能力
 */

#include <dlfcn.h>

#include "lib/commons.h"
#include "lib/NativeObjectFactory.h"
#include "lib/NativeLibrary.h"

namespace NativeTask {

//////////////////////////////////////////////////////////////////
// NativeLibrary methods
//////////////////////////////////////////////////////////////////

/**
 * @brief 原生动态库构造函数
 * @param path 动态库文件路径
 * @param name 动态库名称，用于符号查找前缀
 */
NativeLibrary::NativeLibrary(const string & path, const string & name)
    : _path(path), _name(name), _getObjectCreatorFunc(NULL), _functionGetter(NULL) {
}

/**
 * @brief 加载并初始化原生动态库
 * @return 初始化成功返回true，失败返回false
 * 
 * 动态打开指定路径的动态库，查找并加载对象创建函数、函数获取器和库初始化函数
 * 执行库初始化逻辑，完成后动态库即可提供原生对象和函数
 */
bool NativeLibrary::init() {
  // 使用延迟绑定和全局符号可见性打开动态库
  void *library = dlopen(_path.c_str(), RTLD_LAZY | RTLD_GLOBAL);
  if (NULL == library) {
    LOG("[NativeLibrary] Load object library %s failed.", _path.c_str());
    return false;
  }
  // 清除之前的错误状态
  dlerror();

  // 拼接对象创建器获取函数名
  string create_object_func_name = _name + "GetObjectCreator";
  _getObjectCreatorFunc = (GetObjectCreatorFunc)dlsym(library, create_object_func_name.c_str());
  if (NULL == _getObjectCreatorFunc) {
    LOG("[NativeLibrary] ObjectCreator function [%s] not found", create_object_func_name.c_str());
  }

  // 拼接函数获取器函数名
  string functionGetter = _name + "GetFunctionGetter";
  _functionGetter = (FunctionGetter)dlsym(library, functionGetter.c_str());
  if (NULL == _functionGetter) {
    LOG("[NativeLibrary] function getter [%s] not found", functionGetter.c_str());
  }

  // 拼接库初始化函数名
  string init_library_func_name = _name + "Init";
  InitLibraryFunc init_library_func = (InitLibraryFunc)dlsym(library,
      init_library_func_name.c_str());
  if (NULL == init_library_func) {
    LOG("[NativeLibrary] Library init function [%s] not found", init_library_func_name.c_str());
  } else {
    // 调用动态库初始化函数
    init_library_func();
  }
  return true;
}

/**
 * @brief 根据类名从动态库创建原生对象实例
 * @param clz 要创建的原生对象类名
 * @return 创建成功返回原生对象指针，失败返回NULL
 */
NativeObject * NativeLibrary::createObject(const string & clz) {
  if (NULL == _getObjectCreatorFunc) {
    return NULL;
  }
  return (NativeObject*)((_getObjectCreatorFunc(clz))());
}

/**
 * @brief 根据函数名从动态库获取函数指针
 * @param functionName 要获取的函数名
 * @return 获取成功返回函数指针，失败返回NULL
 */
void * NativeLibrary::getFunction(const string & functionName) {
  if (NULL == _functionGetter) {
    return NULL;
  }
  return (*_functionGetter)(functionName);
}

/**
 * @brief 根据类名获取对应的对象创建器函数
 * @param clz 原生对象类名
 * @return 对象创建器函数指针，失败返回NULL
 */
ObjectCreatorFunc NativeLibrary::getObjectCreator(const string & clz) {
  if (NULL == _getObjectCreatorFunc) {
    return NULL;
  }
  return _getObjectCreatorFunc(clz);
}

} // namespace NativeTask