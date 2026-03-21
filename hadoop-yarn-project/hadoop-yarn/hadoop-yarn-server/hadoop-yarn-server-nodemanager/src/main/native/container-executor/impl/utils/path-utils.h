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
 * @file path-utils.h
 * @brief  YARN NodeManager容器执行器路径工具头文件，提供路径安全校验和目录存在性检查功能
 */

#ifdef __FreeBSD__
#define _WITH_GETLINE
#endif

#ifndef _UTILS_PATH_UTILS_H_
#define _UTILS_PATH_UTILS_H_

/*
 * Verify if a given path is safe or not. For example, we don't want a path
 * include ".." which can do things like:
 * - "/cgroups/cpu,cpuacct/container/../../../etc/passwd"
 *
 * return false/true
 */
/**
 * @brief 校验给定路径是否安全，防止路径遍历攻击（如包含..跳出容器目录）
 * @param path 待校验的路径字符串
 * @return 安全返回1，不安全返回0
 */
int verify_path_safety(const char* path);

/*
 * Verify that a given directory exists.
 * return 0 if the directory exists, 1 if the directory does not exist, and -1
 * for all other errors.
 */
/**
 * @brief 检查给定目录是否存在
 * @param path 待检查的目录路径字符串
 * @return 目录存在返回0，目录不存在返回1，其他错误返回-1
 */
int dir_exists(const char* path);

#endif