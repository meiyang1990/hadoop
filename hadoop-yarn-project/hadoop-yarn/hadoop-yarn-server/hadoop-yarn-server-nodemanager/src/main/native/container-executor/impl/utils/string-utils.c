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
 * @file string-utils.c
 * @brief YARN NodeManager容器执行器 字符串工具函数实现
 * @details 提供字符串解析、验证、格式化、动态字符串缓冲区等常用工具能力
 */
#include "util.h"

#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <limits.h>
#include <errno.h>
#include <stdarg.h>
#include <strings.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#include "string-utils.h"

/**
 * @brief 检查输入字符串是否全部由数字组成
 * @param input 待检查字符串
 * @return 1表示全数字，0表示包含非数字字符
 */
static int all_numbers(char* input) {
  for (; input[0] != 0; input++) {
    if (input[0] < '0' || input[0] > '9') {
      return 0;
    }
  }
  return 1;
}

/**
 * @brief 解析逗号分隔的数字字符串，提取为整数数组
 * @param input 输入逗号分隔字符串
 * @param numbers 输出存储解析结果的整数数组指针
 * @param ret_n_numbers 输出解析得到的数字个数
 * @return 0表示成功，-1表示解析失败
 */
int get_numbers_split_by_comma(const char* input, int** numbers,
                               size_t* ret_n_numbers) {
  // 统计逗号数量，计算需要分配的数组大小
  size_t allocation_size = 1;
  int i = 0;
  while (input[i] != 0) {
    if (input[i] == ',') {
      allocation_size++;
    }
    i++;
  }

  // 分配整数数组内存
  (*numbers) = malloc(sizeof(int) * allocation_size);
  if (!(*numbers)) {
    fprintf(ERRORFILE, "Failed to allocating memory for *numbers: %s\n",
            __func__);
    exit(OUT_OF_MEMORY);
  }
  memset(*numbers, 0, sizeof(int) * allocation_size);

  // 复制输入字符串用于strtok分割
  char* input_cpy = strdup(input);
  if (!input_cpy) {
    fprintf(ERRORFILE, "Failed to allocating memory for input_cpy: %s\n",
            __func__);
    exit(OUT_OF_MEMORY);
  }

  // 按逗号分割逐个解析
  char* p = strtok(input_cpy, ",");
  int idx = 0;
  size_t n_numbers = 0;
  while (p != NULL) {
    char *temp;
    long n = strtol(p, &temp, 0);
    // 按照strtol最佳实践检查转换错误和溢出
    if (temp == p || *temp != '\0' ||
        ((n == LONG_MIN || n == LONG_MAX) && errno == ERANGE)) {
      fprintf(stderr,
              "Could not convert '%s' to long and leftover string is: '%s'\n",
              p, temp);
      free(input_cpy);
      return -1;
    }

    n_numbers++;
    (*numbers)[idx] = n;
    p = strtok(NULL, ",");
    idx++;
  }

  free(input_cpy);
  *ret_n_numbers = n_numbers;

  return 0;
}

/**
 * @brief 验证字符串是否为合法的YARN容器ID
 * @param input 待验证字符串
 * @return 1表示合法容器ID，0表示不合法
 */
int validate_container_id(const char* input) {
  int is_container_id = 1;

  /*
   * 容器ID支持两种格式:
   * container_e17_1410901177871_0001_01_000005 （带应用尝试标识eXX）
   * container_1410901177871_0001_01_000005 （不带应用尝试标识）
   */
  if (!input) {
    return 0;
  }

  char* input_cpy = strdup(input);
  if (!input_cpy) {
    return 0;
  }

  char* p = strtok(input_cpy, "_");
  int idx = 0;
  while (p != NULL) {
    if (0 == idx) {
      // 第一段必须为container
      if (0 != strcmp("container", p)) {
        is_container_id = 0;
        goto cleanup;
      }
    } else if (1 == idx) {
      // 第二段可以是纯数字（老格式） 或者 e开头接数字（带应用尝试）
      if (!all_numbers(p)) {
        if (p[0] == 0) {
          is_container_id = 0;
          goto cleanup;
        }
        if (p[0] != 'e') {
          is_container_id = 0;
          goto cleanup;
        }
        if (!all_numbers(p + 1)) {
          is_container_id = 0;
          goto cleanup;
        }
      }
    } else {
      // 其余段必须全部为数字
      if (!all_numbers(p)) {
        is_container_id = 0;
        goto cleanup;
      }
    }

    p = strtok(NULL, "_");
    idx++;
  }

cleanup:
  if (input_cpy) {
    free(input_cpy);
  }

  // 分割后段数必须为5或6段，否则非法
  if (idx > 6 || idx < 5) {
    is_container_id = 0;
  }
  return is_container_id;
}

/**
 * @brief 根据格式字符串动态分配内存生成字符串
 * @param fmt 格式化字符串，同printf格式
 * @return 分配成功返回堆内存字符串，失败返回NULL，调用者需要释放结果
 */
char *make_string(const char *fmt, ...) {
  va_list vargs;
  va_start(vargs, fmt);
  // 先计算需要的缓冲区大小
  size_t buflen = vsnprintf(NULL, 0, fmt, vargs) + 1;
  va_end(vargs);
  if (buflen <= 0) {
    return NULL;
  }
  // 分配对应大小内存
  char* buf = malloc(buflen);
  if (buf != NULL) {
    va_start(vargs, fmt);
    int ret = vsnprintf(buf, buflen, fmt, vargs);
    va_end(vargs);
    if (ret < 0) {
      free(buf);
      buf = NULL;
    }
  }
  return buf;
}

/**
 * @brief 检查字符串是否以指定后缀结尾
 * @param s 待检查字符串
 * @param suffix 目标后缀
 * @return 非0表示结尾匹配，0表示不匹配
 */
int str_ends_with(const char *s, const char *suffix) {
    size_t slen = strlen(s);
    size_t suffix_len = strlen(suffix);
    return suffix_len <= slen && !strcmp(s + slen - suffix_len, suffix);
}

/**
 * @brief 将4位二进制半字节转换为对应十六进制字符
 * @param nib 4位二进制值
 * @return 对应的十六进制字符（小写）
 */
static char nibble_to_hex(unsigned char nib) {
  return nib < 10 ? '0' + nib : 'a' + nib - 10;
}

/**
 * @brief 将字节数组转换为十六进制字符串
 * @param bytes 输入字节数组
 * @param len 输入字节长度
 * @return 分配成功返回堆内存十六进制字符串，失败返回NULL，调用者需要释放结果
 */
char* to_hexstring(unsigned char* bytes, unsigned int len) {
  char* hexstr = malloc(len * 2 + 1);
  if (hexstr == NULL) {
    return NULL;
  }
  unsigned char* src = bytes;
  char* dest = hexstr;
  // 逐个字节转换为两个十六进制字符
  for (unsigned int i = 0; i < len; ++i) {
    unsigned char val = *src++;
    *dest++ = nibble_to_hex((val >> 4) & 0xF);
    *dest++ = nibble_to_hex(val & 0xF);
  }
  *dest = '\0';
  return hexstr;
}

/**
 * @brief 初始化动态字符串缓冲区
 * @param sb 未初始化的strbuf结构体指针
 * @param initial_capacity 初始容量
 * @return true初始化成功，false内存分配失败
 */
bool strbuf_init(strbuf* sb, size_t initial_capacity) {
  memset(sb, 0, sizeof(*sb));
  char* new_buffer = malloc(initial_capacity);
  if (new_buffer == NULL) {
    return false;
  }
  sb->buffer = new_buffer;
  sb->capacity = initial_capacity;
  sb->length = 0;
  return true;
}

/**
 * @brief 分配并初始化动态字符串缓冲区
 * @param initial_capacity 初始容量
 * @return 分配成功返回strbuf指针，失败返回NULL，调用者需要调用strbuf_free释放
 */
strbuf* strbuf_alloc(size_t initial_capacity) {
  strbuf* sb = malloc(sizeof(*sb));
  if (sb != NULL) {
    if (!strbuf_init(sb, initial_capacity)) {
      free(sb);
      sb = NULL;
    }
  }
  return sb;
}

/**
 * @brief 分离取出动态字符串缓冲区的内部字符缓冲区
 * @param sb strbuf结构体指针
 * @return 返回堆分配的空终止字符串，调用者负责释放结果，原strbuf被重置
 */
char* strbuf_detach_buffer(strbuf* sb) {
  char* result = NULL;
  if (sb != NULL) {
    result = sb->buffer;
    sb->buffer = NULL;
    sb->length = 0;
    sb->capacity = 0;
  }
  return result;
}

/**
 * @brief 销毁动态字符串缓冲区的内部缓冲区，不释放strbuf结构体本身
 * @param sb strbuf结构体指针，适用于栈分配或嵌入其他结构的strbuf
 */
void strbuf_destroy(strbuf* sb) {
  if (sb != NULL) {
    free(sb->buffer);
    sb->buffer = NULL;
    sb->capacity = 0;
    sb->length = 0;
  }
}

/**
 * @brief 释放整个动态字符串缓冲区包括strbuf结构体本身
 * @param sb 待释放的strbuf指针
 */
void strbuf_free(strbuf* sb) {
  if (sb != NULL) {
    strbuf_destroy(sb);
    free(sb);
  }
}

/**
 * @brief 调整动态字符串缓冲区容量
 * @param sb strbuf结构体指针
 * @param new_capacity 新的容量
 * @return true调整成功，false调整失败，新容量小于当前长度会失败
 */
bool strbuf_realloc(strbuf* sb, size_t new_capacity) {
  // 新容量无法容纳已有字符串+终止符，拒绝调整
  if (new_capacity < sb->length + 1) {
    return false;
  }

  char* new_buffer = realloc(sb->buffer, new_capacity);
  if (!new_buffer) {
    return false;
  }

  sb->buffer = new_buffer;
  sb->capacity = new_capacity;
  return true;
}

/**
 * @brief 向动态字符串缓冲区追加格式化字符串
 * @param sb 目标strbuf结构体指针
 * @param realloc_extra 扩容时额外分配的大小，用于减少扩容次数
 * @param fmt 格式化字符串
 * @return true追加成功，false追加失败
 */
bool strbuf_append_fmt(strbuf* sb, size_t realloc_extra,
    const char* fmt, ...) {
  // 长度越界，拒绝操作
  if (sb->length > sb->capacity) {
    return false;
  }

  // 当前已满，先扩容
  if (sb->length == sb->capacity) {
    size_t incr = (realloc_extra == 0) ? 1024 : realloc_extra;
    if (!strbuf_realloc(sb, sb->capacity + incr)) {
      return false;
    }
  }

  // 计算剩余可用空间
  size_t remain = sb->capacity - sb->length;
  va_list vargs;
  va_start(vargs, fmt);
  int needed = vsnprintf(sb->buffer + sb->length, remain, fmt, vargs);
  va_end(vargs);
  if (needed == -1) {
    return false;
  }

  // vsnprintf返回不包含终止符，所以需要额外加1
  needed += 1;
  if (needed > remain) {
    // 当前空间不足，需要重新扩容后再次格式化
    size_t new_size = sb->length + needed + realloc_extra;
    if (!strbuf_realloc(sb, new_size)) {
      return false;
    }
    remain = sb->capacity - sb->length;
    va_start(vargs, fmt);
    needed = vsnprintf(sb->buffer + sb->length, remain, fmt, vargs);
    va_end(vargs);
    if (needed == -1) {
      return false;
    }
    needed += 1;
  }

  // 更新长度，长度不包含终止符
  sb->length += needed - 1;
  return true;
}