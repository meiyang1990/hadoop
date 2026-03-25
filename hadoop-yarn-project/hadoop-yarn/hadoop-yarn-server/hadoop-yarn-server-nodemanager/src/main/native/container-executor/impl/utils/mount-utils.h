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
 * YARN NodeManager 容器执行器挂载工具头文件
 * 提供容器挂载点管理、权限校验相关的数据结构和函数声明
 * 用于在Linux环境下支持容器自定义挂载功能
 */
#ifndef __YARN_POSIX_CONTAINER_EXECUTOR_MOUNT_UTIL_H__
#define __YARN_POSIX_CONTAINER_EXECUTOR_MOUNT_UTIL_H__

/**
 * 挂载选项结构体，存储挂载参数和读写权限
 */
typedef struct mount_options_struct {
  char **opts;          // 挂载选项字符串数组
  unsigned int num_opts; // 挂载选项数量
  unsigned int rw; // 0表示只读，1表示可写
} mount_options;

/**
 * 挂载信息结构体，存储单个挂载点完整信息
 */
typedef struct mount_struct {
    char *src;              // 挂载源路径
    char *dest;             // 挂载目标路径
    mount_options *options; // 挂载选项指针
} mount;

/**
 * 释放挂载选项结构体占用的内存
 * @param options 要释放的挂载选项结构体指针
 */
void free_mount_options(mount_options *options);

/**
 * 释放多个挂载点信息占用的内存
 * @param mounts 挂载点数组指针
 * @param num_mounts 挂载点数量
 */
void free_mounts(mount *mounts, const unsigned int num_mounts);

/**
 * 校验所有挂载点是否符合允许的挂载规则
 * @param permitted_ro_mounts 允许只读挂载的路径列表
 * @param permitted_rw_mounts 允许可写挂载的路径列表
 * @param mounts 待校验的挂载点数组
 * @param num_mounts 待校验的挂载点数量
 * @return 校验通过返回0，不通过返回非零错误码
 */
int validate_mounts(char **permitted_ro_mounts, char **permitted_rw_mounts, mount *mounts, unsigned int num_mounts);

#endif