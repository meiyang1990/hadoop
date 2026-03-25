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
#ifndef RUNC_RUNC_CONFIG_H
#define RUNC_RUNC_CONFIG_H

/**
 * @file runc_config.h
 * @brief YARN NodeManager runC 容器执行器配置项定义
 *
 * 定义了使用 runC 启动隔离容器时所需的所有配置键和默认值，
 * 用于支持基于 runC 的轻量级容器运行时，提供比 Docker 更轻量的容器隔离能力
 */

// runC 配置块在配置文件中的根节点名称
#define CONTAINER_EXECUTOR_CFG_RUNC_SECTION "runc"

// 运行时数据库根目录配置键，建议配置到 tmpfs 等内存文件系统提升性能
#define RUNC_RUN_ROOT_KEY    "runc.run-root"
// runC 运行时根目录默认值
#define DEFAULT_RUNC_ROOT    "/run/yarn-container-executor"

// 宿主机上 runC 可执行文件路径配置键
#define RUNC_BINARY_KEY      "runc.binary"
// runC 可执行文件默认路径
#define DEFAULT_RUNC_BINARY  "/usr/bin/runc"

#endif /* RUNC_RUNC_CONFIG_H */