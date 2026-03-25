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
 * @file NativeTask.cc
 * MapReduce本地任务核心基础实现，提供Native对象类型转换、异常处理、配置管理等基础能力
 * 属于Hadoop MapReduce本地任务模块，为C++本地MapReduce任务提供核心基础设施
 */

#ifndef __CYGWIN__
#include <execinfo.h>
#endif
#include "lib/commons.h"
#include "util/StringUtil.h"
#include "NativeTask.h"
#include "lib/NativeObjectFactory.h"

namespace NativeTask {

//////////////////////////////////////////////////////////////////
// NativeObjectType methods
//////////////////////////////////////////////////////////////////

/**
 * 将Native对象类型枚举转换为字符串描述
 * @param type Native对象类型枚举
 * @return 对应类型的字符串表示
 */
const string NativeObjectTypeToString(NativeObjectType type) {
  switch (type) {
  case BatchHandlerType:
    return string("BatchHandlerType");
  default:
    return string("UnknownObjectType");
  }
}

/**
 * 从字符串解析Native对象类型枚举
 * @param type 类型字符串
 * @return 对应Native对象类型枚举，无法识别返回UnknownObjectType
 */
NativeObjectType NativeObjectTypeFromString(const string type) {
  if (type == "BatchHandlerType") {
    return BatchHandlerType;
  }
  return UnknownObjectType;
}

/**
 * Hadoop异常构造函数，处理异常信息并收集调用栈
 * @param what 原始异常信息
 */
HadoopException::HadoopException(const string & what) {
  // 移除长路径前缀，只保留文件名部分
  size_t n = 0;
  if (what[0] == '/') {
    size_t p = what.find(':');
    if (p != what.npos) {
      while (true) {
        size_t np = what.find('/', n + 1);
        if (np == what.npos || np >= p) {
          break;
        }
        n = np;
      }
    }
  }
  _reason.append(what.c_str() + n, what.length() - n);
  void *array[64];
  size_t size;

#ifndef __CYGWIN__
  // 收集调用栈信息
  size = backtrace(array, 64);
  char ** traces = backtrace_symbols(array, size);
  for (size_t i = 0; i < size; i++) {
    _reason.append("\n\t");
    _reason.append(traces[i]);
  }
#endif
}

///////////////////////////////////////////////////////////

/**
 * 配置类，从指定文件加载配置项
 * @param path 配置文件路径
 */
void Config::load(const string & path) {
  FILE * fin = fopen(path.c_str(), "r");
  if (NULL == fin) {
    THROW_EXCEPTION(IOException, "file not found or can not open for read");
  }
  char buff[256];
  while (fgets(buff, 256, fin) != NULL) {
    // 跳过注释行
    if (buff[0] == '#') {
      continue;
    }
    std::string key = buff;
    if (key[key.length() - 1] == '\n') {
      // 查找等号分隔键值对
      size_t br = key.find('=');
      if (br != key.npos) {
        // 存储键值对，对值去除首尾空白字符
        set(key.substr(0, br), StringUtil::Trim(key.substr(br + 1)));
      }
    }
  }
  fclose(fin);
}

/**
 * 设置配置项键值对
 * @param key 配置键
 * @param value 配置值
 */
void Config::set(const string & key, const string & value) {
  _configs[key] = value;
}

/**
 * 设置整数类型配置项，自动转换为字符串存储
 * @param name 配置键
 * @param value 整数值
 */
void Config::setInt(const string & name, int64_t value) {
  _configs[name] = StringUtil::ToString(value);
}

/**
 * 设置布尔类型配置项，自动转换为字符串存储
 * @param name 配置键
 * @param value 布尔值
 */
void Config::setBool(const string & name, bool value) {
  _configs[name] = StringUtil::ToString(value);
}

/**
 * 从命令行参数解析配置项，参数格式要求为key=value
 * @param argc 参数个数
 * @param argv 参数数组
 */
void Config::parse(int32_t argc, const char ** argv) {
  for (int32_t i = 0; i < argc; i++) {
    // 查找等号分隔键值
    const char * equ = strchr(argv[i], '=');
    if (NULL == equ) {
      LOG("[NativeTask] config argument not recognized: %s", argv[i]);
      continue;
    }
    // 跳过带-前缀的参数
    if (argv[i][0] == '-') {
      LOG("[NativeTask] config argument with '-' prefix ignored: %s", argv[i]);
      continue;
    }
    // 拆分键和值
    string key(argv[i], equ - argv[i]);
    string value(equ + 1, strlen(equ + 1));
    map<string, string>::iterator itr = _configs.find(key);
    if (itr == _configs.end()) {
      // 新增配置项
      _configs[key] = value;
    } else {
      // 同一键多个值用逗号拼接
      itr->second.append(",");
      itr->second.append(value);
    }
  }
}

/**
 * 获取配置项，返回C风格字符串指针
 * @param name 配置键
 * @return 配置值指针，不存在返回NULL
 */
const char * Config::get(const string & name) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return NULL;
  } else {
    return itr->second.c_str();
  }
}

/**
 * 获取配置项，不存在返回默认值
 * @param name 配置键
 * @param defaultValue 默认值
 * @return 配置值字符串
 */
string Config::get(const string & name, const string & defaultValue) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return defaultValue;
  } else {
    return itr->second;
  }
}

/**
 * 获取整数类型配置项，不存在返回默认值
 * @param name 配置键
 * @param defaultValue 默认整数值
 * @return 解析后的整数值
 */
int64_t Config::getInt(const string & name, int64_t defaultValue) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return defaultValue;
  } else {
    return StringUtil::toInt(itr->second);
  }
}

/**
 * 获取布尔类型配置项，不存在返回默认值
 * @param name 配置键
 * @param defaultValue 默认布尔值
 * @return 解析后的布尔值
 */
bool Config::getBool(const string & name, bool defaultValue) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return defaultValue;
  } else {
    return StringUtil::toBool(itr->second);
  }
}

/**
 * 获取浮点类型配置项，不存在返回默认值
 * @param name 配置键
 * @param defaultValue 默认浮点值
 * @return 解析后的浮点值
 */
float Config::getFloat(const string & name, float defaultValue) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return defaultValue;
  } else {
    return StringUtil::toFloat(itr->second);
  }
}

/**
 * 获取多值字符串配置，按逗号分割到目标vector
 * @param name 配置键
 * @param dest 输出vector
 */
void Config::getStrings(const string & name, vector<string> & dest) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr != _configs.end()) {
    StringUtil::Split(itr->second, ",", dest, true);
  }
}

/**
 * 获取多值整数配置，按逗号分割解析到目标vector
 * @param name 配置键
 * @param dest 输出整数vector
 */
void Config::getInts(const string & name, vector<int64_t> & dest) {
  vector<string> sdest;
  getStrings(name, sdest);
  for (size_t i = 0; i < sdest.size(); i++) {
    dest.push_back(StringUtil::toInt(sdest[i]));
  }
}

/**
 * 获取多值浮点数配置，按逗号分割解析到目标vector
 * @param name 配置键
 * @param dest 输出浮点数vector
 */
void Config::getFloats(const string & name, vector<float> & dest) {
  vector<string> sdest;
  getStrings(name, sdest);
  for (size_t i = 0; i < sdest.size(); i++) {
    dest.push_back(StringUtil::toFloat(sdest[i]));
  }
}

///////////////////////////////////////////////////////////

/**
 * 处理器基类默认计数器获取实现，不提供实际计数器
 * @param group 计数器组
 * @param name 计数器名称
 * @return 始终返回NULL
 */
Counter * ProcessorBase::getCounter(const string & group, const string & name) {
  return NULL;
}

///////////////////////////////////////////////////////////

} // namespace NativeTask