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
 * @file string-utils.h
 * @brief YARN NodeManager 容器执行器字符串处理工具头文件
 * 提供字符串解析、验证、可变字符串缓冲区等常用工具函数
 */

#ifdef __FreeBSD__
// FreeBSD系统需要定义该宏以启用getline函数
#define _WITH_GETLINE
#endif

#ifndef _UTILS_STRING_UTILS_H_
#define _UTILS_STRING_UTILS_H_

#include <stdbool.h>
#include <stddef.h>

/**
 * @brief 可变字符串缓冲区结构
 * 用于动态拼接字符串，自动管理内存
 */
typedef struct strbuf_struct {
  char* buffer;               // 指向字符串起始位置
  size_t length;              // 当前字符串长度（不含末尾NUL终止符）
  size_t capacity;            // 当前缓冲区分配的总容量
} strbuf;


/**
 * @brief 验证容器ID格式是否合法
 * @param input 待验证的输入字符串
 * @return 返回1表示合法，0表示非法
 */
int validate_container_id(const char* input);

/**
 * @brief 从逗号分隔的输入字符串中解析出整数数组
 * @param input 输入字符串，整数以逗号分隔
 * @param numbers 输出参数，用于存储解析出的整数数组指针
 * @param n_numbers 输出参数，存储解析出的整数个数
 * @return 返回0表示解析成功，非0表示解析失败
 */
int get_numbers_split_by_comma(const char* input, int** numbers, size_t* n_numbers);

/**
 * @brief 根据格式化字符串动态生成堆内存字符串
 * @param fmt 格式化字符串，同printf格式
 * @return 生成的堆内存字符串，需要调用者释放
 */
char *make_string(const char *fmt, ...);

/**
 * @brief 判断字符串是否以指定后缀结尾
 * @param s 待判断的输入字符串
 * @param suffix 目标后缀
 * @return 返回1表示字符串以该后缀结尾，0表示不匹配
 */
int str_ends_with(const char *s, const char *suffix);

/**
 * @brief 将字节数组转换为十六进制字符串
 * @param bytes 输入字节数组
 * @param len 输入字节长度
 * @return 成功返回分配的十六进制字符串指针，失败返回NULL，需要调用者释放
 */
char* to_hexstring(unsigned char* bytes, unsigned int len);

/**
 * @brief 在堆上分配并初始化一个指定初始容量的strbuf
 * @param initial_capacity 初始容量大小
 * @return 成功返回strbuf指针，失败返回NULL，需要调用strbuf_free释放
 */
strbuf* strbuf_alloc(size_t initial_capacity);

/**
 * @brief 初始化已分配的strbuf结构体，指定初始容量
 * 适用于栈分配的strbuf或嵌入在其他结构体中的strbuf
 * @param sb 待初始化的strbuf指针
 * @param initial_capacity 初始容量大小
 * @return 成功返回true，内存分配失败返回false
 */
bool strbuf_init(strbuf* sb, size_t initial_capacity);

/**
 * @brief 调整strbuf容量到指定新容量
 * @param sb 目标strbuf指针
 * @param new_capacity 新容量大小
 * @return 调整成功返回true，失败返回false
 */
bool strbuf_realloc(strbuf* sb, size_t new_capacity);

/**
 * @brief 从strbuf中分离出底层字符缓冲区
 * 调用后strbuf不再持有该缓冲区，控制权转移给调用者
 * @param sb 目标strbuf指针
 * @return 以堆分配的空终止字符缓冲区，调用者负责释放
 */
char* strbuf_detach_buffer(strbuf* sb);

/**
 * @brief 销毁strbuf持有的底层内存，但不释放strbuf结构体本身
 * 适用于栈分配或嵌入式strbuf结构
 * @param sb 目标strbuf指针
 */
void strbuf_destroy(strbuf* sb);

/**
 * @brief 释放strbuf结构体及其持有的所有内存
 * @param sb 待释放的strbuf指针，适用于堆分配的strbuf
 */
void strbuf_free(strbuf* sb);

/**
 * @brief 向strbuf追加格式化字符串
 * @param sb 目标strbuf指针
 * @param realloc_extra 重新分配内存时额外扩展的容量，减少后续扩容次数
 * @param fmt 格式化字符串，同printf格式
 * @return 追加成功返回true，内存分配失败返回false
 */
bool strbuf_append_fmt(strbuf* sb, size_t realloc_extra, const char* fmt, ...);

#endif