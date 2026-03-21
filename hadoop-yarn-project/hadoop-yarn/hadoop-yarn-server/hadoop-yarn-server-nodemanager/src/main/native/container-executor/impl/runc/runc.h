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
 * YARN NodeManager runC 容器执行器头文件
 * 提供基于 runC 的容器生命周期管理接口，支持使用OCI runC 运行YARN容器
 */
#ifndef RUNC_RUNC_H
#define RUNC_RUNC_H

#include <stdbool.h>

/**
 * 检查runC模块是否在配置中启用
 * @param conf 节点管理器配置对象指针
 * @return 1 表示启用，0 表示禁用
 */
int runc_module_enabled(const struct configuration *conf);

/**
 * 通过runC启动并运行YARN容器
 * @param command_file 容器启动命令配置文件路径
 * @return 0 表示执行成功，非0表示执行失败
 */
int run_runc_container(const char* command_file);

#endif /* RUNC_RUNC_H */