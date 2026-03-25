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
 * @file file-utils.h
 * YARN NodeManager 容器执行器文件工具模块头文件
 * 提供文件读取、写入等基础文件操作工具函数，支持切换NodeManager用户权限操作
 */
#ifndef UTILS_FILE_UTILS_H
#define UTILS_FILE_UTILS_H

#include <stdbool.h>

/**
 * 读取指定文件内容到堆分配缓冲区，返回以NUL结尾的字符串
 * 注意：文件内容不能包含NUL字符，否则结果会被截断
 *
 * @param filename 要读取的文件路径
 * @return 分配好的NUL结尾字符串指针，出错返回NULL
 */
char* read_file_to_string(const char* filename);

/**
 * 以YARN NodeManager用户身份读取文件内容为字符串
 * 更多细节请参考read_file_to_string
 *
 * @param filename 要读取的文件路径
 * @return 分配好的NUL结尾字符串指针，出错返回NULL
 */
char* read_file_to_string_as_nm_user(const char* filename);

/**
 * 以YARN NodeManager用户身份将字节序列写入新文件
 *
 * @param path 目标文件路径
 * @param data 要写入的数据缓冲区指针
 * @param count 要写入的字节数
 * @return 写入成功返回true，出错返回false
 */
bool write_file_as_nm(const char* path, const void* data, size_t count);

#endif /* UTILS_FILE_UTILS_H */