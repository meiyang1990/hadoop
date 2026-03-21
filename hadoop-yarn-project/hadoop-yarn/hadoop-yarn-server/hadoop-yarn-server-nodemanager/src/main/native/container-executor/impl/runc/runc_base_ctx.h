// 这个文件已经全部加上中文注释
/*
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
 * @file runc_base_ctx.h
 * @brief YARN NodeManager 容器执行器 runC 基础上下文管理头文件
 * 负责管理 runC 运行时的根目录、镜像层锁等基础资源，为overlayfs镜像层管理提供并发控制和路径处理能力
 */
#ifndef RUNC_RUNC_BASE_CTX_H
#define RUNC_RUNC_BASE_CTX_H

#include <stdbool.h>

// 镜像层基础名称长度，等于SHA256哈希的十六进制字符串长度
#define LAYER_NAME_LENGTH       64

// NOTE: 修改此结构体时需要同步更新init_runc_base_ctx和destroy_runc_base_ctx函数
/**
 * @brief runC基础上下文结构体，保存runC运行时核心资源信息
 */
typedef struct runc_base_ctx_struct {
  char* run_root;             // runC运行时文件系统根目录路径
  int layers_lock_fd;         // 镜像层锁文件的文件描述符
  int layers_lock_state;      // 当前锁状态：F_RDLCK(读锁)、F_WRLCK(写锁) 或 F_UNLCK(无锁)
} runc_base_ctx;


/**
 * 分配并初始化一个runC基础上下文
 *
 * 返回值：分配初始化完成的上下文指针，出错返回NULL
 */
runc_base_ctx* alloc_runc_base_ctx();

/**
 * 释放runC基础上下文以及关联的所有内存
 */
void free_runc_base_ctx(runc_base_ctx* ctx);

/**
 * 初始化未被初始化的runC基础上下文
 */
void init_runc_base_ctx(runc_base_ctx* ctx);

/**
 * 释放runC基础上下文持有的资源，但不释放结构体本身。
 * 适用于栈分配的上下文或者嵌入其他结构的上下文场景。
 * 堆分配的上下文应使用free_runc_base_ctx释放。
 */
void destroy_runc_base_ctx(runc_base_ctx* ctx);

/**
 * 打开基础上下文准备使用。如果不存在，会创建容器运行时根目录和镜像层锁文件
 *
 * 返回值：成功返回true，出错返回false
 */
bool open_runc_base_ctx(runc_base_ctx* ctx);

/**
 * 分配并打开一个基础上下文
 *
 * 返回值：创建好的上下文指针，出错返回NULL
 */
runc_base_ctx* setup_runc_base_ctx();

/**
 * 获取镜像层读锁
 *
 * 返回值：成功返回true，出错返回false
 */
bool acquire_runc_layers_read_lock(runc_base_ctx* ctx);

/**
 * 获取镜像层写锁
 *
 * 返回值：成功返回true，出错返回false
 */
bool acquire_runc_layers_write_lock(runc_base_ctx* ctx);

/**
 * 释放镜像层锁
 *
 * 返回值：成功返回true，出错返回false
 */
bool release_runc_layers_lock(runc_base_ctx* ctx);

/**
 * 获取runC运行时镜像层目录路径
 *
 * 返回值：堆分配的镜像层目录路径字符串，出错返回NULL
 */
char* get_runc_layers_path(const char* run_root);

/**
 * 获取单个镜像层目录路径
 *
 * 返回值：堆分配的镜像层目录路径字符串，出错返回NULL
 */
char* get_runc_layer_path(const char* run_root, const char* layer_name);

/**
 * 获取镜像层挂载点路径
 *
 * 返回值：堆分配的镜像层挂载点路径字符串，出错返回NULL
 */
char* get_runc_layer_mount_path(const char* layer_path);

/**
 * 从镜像层挂载点路径反推镜像层目录路径
 *
 * 返回值：堆分配的镜像层目录路径字符串，出错返回NULL
 */
char* get_runc_layer_path_from_mount_path(const char* mount_path);

#endif /* RUNC_RUNC_BASE_CTX_H */