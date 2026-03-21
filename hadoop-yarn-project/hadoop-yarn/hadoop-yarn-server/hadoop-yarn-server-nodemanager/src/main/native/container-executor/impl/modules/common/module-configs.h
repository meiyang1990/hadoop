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
 * YARN NodeManager 容器执行器可扩展模块通用配置头文件
 * 本文件提供模块启用状态检查功能，支持模块化扩展容器执行器能力
 */

#ifdef __FreeBSD__
#define _WITH_GETLINE
#endif

#ifndef _MODULES_COMMON_MODULE_CONFIGS_H_
#define _MODULES_COMMON_MODULE_CONFIGS_H_

#include "configuration.h"

/**
 * 根据模块名称检查指定模块是否启用
 * @param section_cfg 配置段指针，对应模块所在的配置段
 * @param module_name 待检查的模块名称
 * @return 0 表示模块禁用，非0表示模块启用
 */
int module_enabled(const struct section* section_cfg, const char* module_name);

#endif