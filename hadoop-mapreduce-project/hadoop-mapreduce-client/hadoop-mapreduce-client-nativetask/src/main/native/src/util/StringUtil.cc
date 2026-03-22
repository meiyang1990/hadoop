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
 * @file StringUtil.cc
 * @brief 字符串工具类实现，提供NativeTask模块中常用的字符串转换、格式化、分割、拼接等操作
 */

#include <stdarg.h>
#include "lib/commons.h"
#include "util/StringUtil.h"

namespace NativeTask {

/**
 * @brief 将32位有符号整数转换为字符串
 * @param v 待转换的整数
 * @return 转换后的字符串
 */
string StringUtil::ToString(int32_t v) {
  char tmp[32];
  snprintf(tmp, 32, "%d", v);
  return tmp;
}

/**
 * @brief 将32位无符号整数转换为字符串
 * @param v 待转换的无符号整数
 * @return 转换后的字符串
 */
string StringUtil::ToString(uint32_t v) {
  char tmp[32];
  snprintf(tmp, 32, "%u", v);
  return tmp;
}

/**
 * @brief 将64位有符号整数转换为字符串
 * @param v 待转换的64位整数
 * @return 转换后的字符串
 */
string StringUtil::ToString(int64_t v) {
  char tmp[32];
  snprintf(tmp, 32, "%"PRId64, v);
  return tmp;
}

/**
 * @brief 将64位有符号整数转换为指定长度、左侧补填充字符的字符串
 * @param v 待转换的64位整数
 * @param pad 填充字符
 * @param len 输出字符串总长度
 * @return 格式化补全后的字符串
 */
string StringUtil::ToString(int64_t v, char pad, int64_t len) {
  char tmp[32];
  snprintf(tmp, 32, "%%%c%"PRId64""PRId64, pad, len);
  return Format(tmp, v);
}

/**
 * @brief 将64位无符号整数转换为字符串
 * @param v 待转换的64位无符号整数
 * @return 转换后的字符串
 */
string StringUtil::ToString(uint64_t v) {
  char tmp[32];
  snprintf(tmp, 32, "%"PRIu64, v);
  return tmp;
}

/**
 * @brief 将布尔值转换为字符串
 * @param v 待转换的布尔值
 * @return "true" 或 "false"
 */
string StringUtil::ToString(bool v) {
  if (v) {
    return "true";
  } else {
    return "false";
  }
}

/**
 * @brief 将单精度浮点数转换为字符串
 * @param v 待转换的浮点数
 * @return 转换后的字符串
 */
string StringUtil::ToString(float v) {
  return Format("%f", v);
}

/**
 * @brief 将双精度浮点数转换为字符串
 * @param v 待转换的双精度浮点数
 * @return 转换后的字符串
 */
string StringUtil::ToString(double v) {
  return Format("%lf", v);
}

/**
 * @brief 将二进制字节数组转换为十六进制字符串
 * @param v 输入二进制数据指针
 * @param len 二进制数据长度
 * @return 每个字节对应两个十六进制字符的字符串
 */
string StringUtil::ToHexString(const void * v, uint32_t len) {
  string ret = string(len * 2, '0');
  for (uint32_t i = 0; i < len; i++) {
    snprintf(&(ret[i*2]), 3, "%02x", ((char*)v)[i]);
  }
  return ret;
}

/**
 * @brief 将字符串转换为布尔值
 * @param str 输入字符串
 * @return 字符串等于"true"返回true，否则返回false
 */
bool StringUtil::toBool(const string & str) {
  if (str == "true") {
    return true;
  } else {
    return false;
  }
}

/**
 * @brief 将字符串转换为64位整数
 * @param str 输入数字字符串
 * @return 转换后的64位整数
 */
int64_t StringUtil::toInt(const string & str) {
  return strtoll(str.c_str(), NULL, 10);
}

/**
 * @brief 将字符串转换为单精度浮点数
 * @param str 输入浮点数字符串
 * @return 转换后的单精度浮点数
 */
float StringUtil::toFloat(const string & str) {
  return strtof(str.c_str(), NULL);
}

/**
 * @brief printf风格格式化字符串，返回结果
 * @param fmt 格式化字符串
 * @param ... 可变参数列表
 * @return 格式化后的字符串
 */
string StringUtil::Format(const char * fmt, ...) {
  char tmp[256];
  string dest;
  va_list al;
  va_start(al, fmt);
  // 先尝试用栈上缓冲区格式化
  int len = vsnprintf(tmp, 255, fmt, al);
  va_end(al);
  // 如果超出栈缓冲区大小，动态分配堆内存
  if (len > 255) {
    char * destbuff = new char[len + 1];
    va_start(al, fmt);
    len = vsnprintf(destbuff, len + 1, fmt, al);
    va_end(al);
    dest.append(destbuff, len);
    delete [] destbuff;
  } else {
    dest.append(tmp, len);
  }
  return dest;
}

/**
 * @brief printf风格格式化字符串，追加到目标字符串
 * @param dest 目标字符串，结果将追加到此处
 * @param fmt 格式化字符串
 * @param ... 可变参数列表
 */
void StringUtil::Format(string & dest, const char * fmt, ...) {
  char tmp[256];
  va_list al;
  va_start(al, fmt);
  // 先尝试用栈上缓冲区格式化
  int len = vsnprintf(tmp, 255, fmt, al);
  // 如果超出栈缓冲区大小，动态分配堆内存
  if (len > 255) {
    char * destbuff = new char[len + 1];
    len = vsnprintf(destbuff, len + 1, fmt, al);
    dest.append(destbuff, len);
  } else {
    dest.append(tmp, len);
  }
  va_end(al);
}

/**
 * @brief 将字符串中所有字符转为小写
 * @param name 输入原字符串
 * @return 全小写的新字符串
 */
string StringUtil::ToLower(const string & name) {
  string ret = name;
  for (size_t i = 0; i < ret.length(); i++) {
    ret.at(i) = ::tolower(ret[i]);
  }
  return ret;
}

/**
 * @brief 去除字符串两端的空白字符
 * @param str 输入原字符串
 * @return 去除两端空白后的新字符串
 */
string StringUtil::Trim(const string & str) {
  if (str.length() == 0) {
    return str;
  }
  size_t l = 0;
  // 找到第一个非空白字符位置
  while (l < str.length() && isspace(str[l])) {
    l++;
  }
  // 全空白字符返回空串
  if (l >= str.length()) {
    return string();
  }
  // 找到最后一个非空白字符位置
  size_t r = str.length();
  while (isspace(str[r - 1])) {
    r--;
  }
  return str.substr(l, r - l);
}

/**
 * @brief 按指定分隔符分割字符串，结果存入目标vector
 * @param src 待分割的源字符串
 * @param sep 分隔符字符串
 * @param dest 输出结果vector
 * @param clean 是否清洗结果：true则对分割后的子串做trim并忽略空结果，false保留原分割结果
 */
void StringUtil::Split(const string & src, const string & sep, vector<string> & dest, bool clean) {
  if (sep.length() == 0) {
    return;
  }
  size_t cur = 0;
  while (true) {
    size_t pos;
    // 优化单字符分隔符查找
    if (sep.length() == 1) {
      pos = src.find(sep[0], cur);
    } else {
      pos = src.find(sep, cur);
    }
    string add = src.substr(cur, pos - cur);
    if (clean) {
      string trimed = Trim(add);
      if (trimed.length() > 0) {
        dest.push_back(trimed);
      }
    } else {
      dest.push_back(add);
    }
    if (pos == string::npos) {
      break;
    }
    cur = pos + sep.length();
  }
}

/**
 * @brief 将字符串vector用指定分隔符拼接为一个字符串
 * @param strs 待拼接的字符串列表
 * @param sep 分隔符
 * @return 拼接完成的字符串
 */
string StringUtil::Join(const vector<string> & strs, const string & sep) {
  string ret;
  for (size_t i = 0; i < strs.size(); i++) {
    if (i > 0) {
      ret.append(sep);
    }
    ret.append(strs[i]);
  }
  return ret;
}

/**
 * @brief 判断字符串是否以指定前缀开头
 * @param str 输入字符串
 * @param prefix 待匹配前缀
 * @return true 字符串以prefix开头，false否则
 */
bool StringUtil::StartsWith(const string & str, const string & prefix) {
  if ((prefix.length() > str.length())
      || (memcmp(str.data(), prefix.data(), prefix.length()) != 0)) {
    return false;
  }
  return true;
}

/**
 * @brief 判断字符串是否以指定后缀结尾
 * @param str 输入字符串
 * @param suffix 待匹配后缀
 * @return true 字符串以suffix结尾，false否则
 */
bool StringUtil::EndsWith(const string & str, const string & suffix) {
  if ((suffix.length() > str.length()) ||
      (memcmp(str.data() + str.length() - suffix.length(),
              suffix.data(), suffix.length()) != 0)) {
    return false;
  }
  return true;
}

} // namespace NativeTask