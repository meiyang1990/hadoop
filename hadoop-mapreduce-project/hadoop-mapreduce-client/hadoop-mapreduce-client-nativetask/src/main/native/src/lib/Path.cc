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
 * @file Path.cc
 * Hadoop MapReduce本地任务路径工具类实现，提供HDFS路径相关的基础操作
 */

#include "lib/Path.h"

namespace NativeTask {

/**
 * 判断给定路径是否为绝对路径
 * @param path 待检查的路径字符串
 * @return true 如果是绝对路径，否则false
 */
bool Path::IsAbsolute(const string & path) {
  if (path.length() > 0 && path[0] == '/') {
    return true;
  }
  return false;
}

/**
 * 获取路径的父目录路径
 * @param path 原路径字符串
 * @return 父目录路径，根目录返回空字符串，无斜杠返回当前目录标识"."
 */
string Path::GetParent(const string & path) {
  // 查找最后一个斜杠位置
  size_t lastSlash = path.rfind('/');
  // 路径中没有斜杠，返回当前目录标识
  if (lastSlash == path.npos) {
    return ".";
  }
  // 根目录"/"，返回空路径
  if (lastSlash == 0 && path.length() == 1) {
    return "";
  }
  // 根目录下路径，直接返回原路径（根目录）
  if (lastSlash == 0) {
    return path;
  }
  // 截取到最后一个斜杠位置，得到父目录
  return path.substr(0, lastSlash);
}

/**
 * 获取路径中的文件名部分
 * @param path 完整路径字符串
 * @return 文件名部分，无斜杠则返回整个路径
 */
string Path::GetName(const string & path) {
  // 查找最后一个斜杠位置
  size_t lastSlash = path.rfind('/');
  // 没有斜杠，整个路径就是文件名
  if (lastSlash == path.npos) {
    return path;
  }
  // 截取最后一个斜杠之后的部分作为文件名
  return path.substr(lastSlash + 1);
}

} // namespace NativeTask