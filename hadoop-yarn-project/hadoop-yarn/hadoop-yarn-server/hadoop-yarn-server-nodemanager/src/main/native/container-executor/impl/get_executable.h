// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain obtain a copy of the License at
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
 * @file get_executable.h
 * @brief YARN NodeManager容器执行器 获取当前可执行文件路径工具头文件
 * 属于YARN本地容器执行器模块，用于获取当前运行程序的绝对路径
 */

#ifndef __YARN_POSIX_CONTAINER_EXECUTOR_GET_EXECUTABLE_H__
#define __YARN_POSIX_CONTAINER_EXECUTOR_GET_EXECUTABLE_H__

/**
 * 获取当前正在运行的可执行文件的路径
 * @param argv0 主函数传入的可执行文件名称参数
 * @return 当前运行可执行文件的路径字符串，调用者需负责释放内存
 */
char* get_executable(char *argv0);

#endif