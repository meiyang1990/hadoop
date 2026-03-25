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
 * @file devices-module.h
 * YARN NodeManager 容器执行器设备管控模块头文件
 * 负责控制容器对Linux设备的访问权限，通过cgroups实现设备访问隔离
 */

#ifdef __FreeBSD__
#define _WITH_GETLINE
#endif

#ifndef _MODULES_DEVICES_MUDULE_H_
#define _MODULES_DEVICES_MUDULE_H_

// 配置项：禁止访问的设备号列表，格式为 "major1:minor1,major2:minor2"
#define DEVICES_DENIED_NUMBERS "devices.denied-numbers"
// 设备管控模块配置段名称
#define DEVICES_MODULE_SECTION_NAME "devices"

// 函数指针类型定义，用于单元测试桩替换cgroups参数更新操作
typedef int (*update_cgroups_param_function)(const char*, const char*,
   const char*, const char*);

/**
 * 处理容器设备访问请求，更新cgroups设备权限配置
 * @param func 更新cgroups参数的函数指针，支持测试注入
 * @param module_name 模块名称
 * @param module_argc 请求参数个数
 * @param module_argv 请求参数数组
 * @return 处理成功返回0，失败返回非0错误码
 */
int handle_devices_request(update_cgroups_param_function func,
   const char* module_name, int module_argc, char** module_argv);

/**
 * 从文件系统重新加载设备管控配置，仅对测试可见
 */
void reload_devices_configuration();

#endif