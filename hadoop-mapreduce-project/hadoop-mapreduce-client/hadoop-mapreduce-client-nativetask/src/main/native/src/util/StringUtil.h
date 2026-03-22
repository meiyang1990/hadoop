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
 * @file StringUtil.h
 * @brief 原生MapReduce任务工具模块 - 字符串工具类头文件
 *
 * 提供Hadoop原生任务中常用的字符串转换、格式化、分割、拼接等通用工具方法
 */

#ifndef STRINGUTIL_H_
#define STRINGUTIL_H_

#include <stdint.h>
#include <vector>
#include <string>

namespace NativeTask {

using std::vector;
using std::string;

/**
 * @class StringUtil
 * @brief 字符串工具类，提供原生任务中各类字符串处理静态方法
 *
 * 核心职责：封装常用的类型与字符串相互转换、格式化、文本处理等通用操作，
 * 为原生MapReduce任务提供统一的字符串处理能力，避免重复实现基础逻辑
 */
class StringUtil {
public:
  /**
   * @brief 将32位有符号整数转换为字符串
   * @param v 待转换的整数
   * @return 转换后的字符串
   */
  static string ToString(int32_t v);

  /**
   * @brief 将32位无符号整数转换为字符串
   * @param v 待转换的整数
   * @return 转换后的字符串
   */
  static string ToString(uint32_t v);

  /**
   * @brief 将64位有符号整数转换为字符串
   * @param v 待转换的整数
   * @return 转换后的字符串
   */
  static string ToString(int64_t v);

  /**
   * @brief 将64位有符号整数转换为固定长度字符串，不足则补填充字符
   * @param v 待转换的整数
   * @param pad 填充字符
   * @param len 目标字符串总长度
   * @return 填充后的固定长度字符串
   */
  static string ToString(int64_t v, char pad, int64_t len);

  /**
   * @brief 将64位无符号整数转换为字符串
   * @param v 待转换的整数
   * @return 转换后的字符串
   */
  static string ToString(uint64_t v);

  /**
   * @brief 将布尔值转换为字符串("true"/"false")
   * @param v 待转换的布尔值
   * @return 转换后的字符串
   */
  static string ToString(bool v);

  /**
   * @brief 将单精度浮点数转换为字符串
   * @param v 待转换的浮点数
   * @return 转换后的字符串
   */
  static string ToString(float v);

  /**
   * @brief 将双精度浮点数转换为字符串
   * @param v 待转换的浮点数
   * @return 转换后的字符串
   */
  static string ToString(double v);

  /**
   * @brief 将二进制字节数组转换为十六进制字符串
   * @param v 二进制数据指针
   * @param len 二进制数据长度
   * @return 十六进制字符串
   */
  static string ToHexString(const void * v, uint32_t len);

  /**
   * @brief 将字符串解析为64位整数
   * @param str 待解析的输入字符串
   * @return 解析得到的整数值
   */
  static int64_t toInt(const string & str);

  /**
   * @brief 将字符串解析为布尔值
   * @param str 待解析的输入字符串，"true"/"false"不区分大小写
   * @return 解析得到的布尔值
   */
  static bool toBool(const string & str);

  /**
   * @brief 将字符串解析为单精度浮点数
   * @param str 待解析的输入字符串
   * @return 解析得到的浮点值
   */
  static float toFloat(const string & str);

  /**
   * @brief 使用printf风格格式字符串生成新字符串
   * @param fmt printf格式字符串
   * @param ... 可变参数列表
   * @return 格式化后的字符串
   */
  static string Format(const char * fmt, ...);

  /**
   * @brief 使用printf风格格式化字符串，输出到已有字符串对象
   * @param dest 输出目标字符串对象
   * @param fmt printf格式字符串
   * @param ... 可变参数列表
   */
  static void Format(string & dest, const char * fmt, ...);

  /**
   * @brief 将输入字符串转换为全小写
   * @param name 输入原始字符串
   * @return 全小写字符串
   */
  static string ToLower(const string & name);

  /**
   * @brief 去除字符串首尾空白字符
   * @param str 输入原始字符串
   * @return 去除首尾空白后的字符串
   */
  static string Trim(const string & str);

  /**
   * @brief 按分隔符分割字符串，结果存入目标vector
   * @param src 待分割的源字符串
   * @param sep 分隔符字符串
   * @param dest 输出结果vector
   * @param clean 是否清除分割后得到的空字符串，默认为false
   */
  static void Split(const string & src, const string & sep, vector<string> & dest,
      bool clean = false);

  /**
   * @brief 将字符串数组按分隔符拼接为单个字符串
   * @param strs 待拼接的字符串vector
   * @param sep 分隔符字符串
   * @return 拼接完成的字符串
   */
  static string Join(const vector<string> & strs, const string & sep);

  /**
   * @brief 判断字符串是否以指定前缀开头
   * @param str 输入字符串
   * @param prefix 待检查的前缀
   * @return true 是前缀匹配；false 不匹配
   */
  static bool StartsWith(const string & str, const string & prefix);

  /**
   * @brief 判断字符串是否以指定后缀结尾
   * @param str 输入字符串
   * @param suffix 待检查的后缀
   * @return true 是后缀匹配；false 不匹配
   */
  static bool EndsWith(const string & str, const string & suffix);
};

} // namespace NativeTask

#endif /* STRINGUTIL_H_ */