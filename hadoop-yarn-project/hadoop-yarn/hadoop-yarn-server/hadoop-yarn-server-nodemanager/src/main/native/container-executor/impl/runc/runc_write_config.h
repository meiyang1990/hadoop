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
 * @file runc_write_config.h
 * @brief YARN NodeManager runC容器配置文件生成模块头文件
 * @details 负责生成符合runC规范的容器运行时配置JSON，并写入配置文件
 */
#ifndef RUNC_RUNC_WRITE_CONFIG_H
#define RUNC_RUNC_WRITE_CONFIG_H

/**
 * 构建runC运行时配置JSON对象
 *
 * @param rlc runC容器启动命令参数结构体指针
 * @param rootfs_path 容器根文件系统路径
 * @return 构建成功返回cJSON对象指针，失败返回NULL
 */
cJSON* build_runc_config_json(const runc_launch_cmd* rlc,
                               const char* rootfs_path);

/**
 * 生成并写入runC容器运行时配置文件到磁盘
 *
 * @param rlc runC容器启动命令参数结构体指针
 * @param rootfs_path 容器根文件系统路径
 * @return 写入成功返回配置文件路径字符串，失败返回NULL，调用者需要释放返回的内存
 */
char* write_runc_runc_config(const runc_launch_cmd* rlc, const char* rootfs_path);

#endif /* RUNC_RUNC_WRITE_CONFIG_H */