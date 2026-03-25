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
 * @file Path.h
 * @brief Hadoop本地任务路径处理工具类头文件，提供HDFS路径相关的静态工具方法
 */

#ifndef PATH_H_
#define PATH_H_

#include <stdint.h>
#include <string>

namespace NativeTask {

using std::string;

/**
 * @class Path
 * @brief 路径处理工具类，提供Hadoop文件系统路径的解析操作，所有方法均为静态方法
 */
class Path {
public:
  /**
   * @brief 判断给定路径是否为绝对路径
   * @param path 待检查的路径字符串
   * @return true 路径是绝对路径，false 路径是相对路径
   */
  static bool IsAbsolute(const string & path);

  /**
   * @brief 获取给定路径的父目录路径
   * @param path 输入路径字符串
   * @return 父目录的路径字符串
   */
  static string GetParent(const string & path);

  /**
   * @brief 获取给定路径中的文件名部分（最后一级路径名）
   * @param path 输入路径字符串
   * @return 路径对应的文件名
   */
  static string GetName(const string & path);
};

} // namespace NativeTask

#endif /* PATH_H_ */