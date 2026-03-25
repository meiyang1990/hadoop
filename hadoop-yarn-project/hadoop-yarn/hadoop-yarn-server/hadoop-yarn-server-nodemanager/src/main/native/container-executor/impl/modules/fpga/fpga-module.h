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
 * @file fpga-module.h
 * YARN NodeManager 容器执行器 FPGA 设备管理模块头文件
 * 负责在YARN容器启动时，为容器分配隔离指定的FPGA设备，通过cgroups实现设备资源隔离
 */

#ifdef __FreeBSD__
#define _WITH_GETLINE
#endif

#ifndef _MODULES_FPGA_FPGA_MUDULE_H_
#define _MODULES_FPGA_FPGA_MUDULE_H_

// FPGA设备主设备号配置项键名
#define FPGA_MAJOR_NUMBER_CONFIG_KEY "fpga.major-device-number"
// 允许分配的FPGA设备次设备号列表配置项键名
#define FPGA_ALLOWED_DEVICES_MINOR_NUMBERS "fpga.allowed-device-minor-numbers"
// FPGA模块配置段名称
#define FPGA_MODULE_SECTION_NAME "fpga"

// 用于单元测试桩替换：更新cgroups参数的函数指针类型定义
typedef int (*update_cgroups_parameters_function)(const char*, const char*,
   const char*, const char*);

/**
 * 处理FPGA设备分配请求
 * @param func 更新cgroups参数的函数指针，用于设备权限配置
 * @param module_name 模块名称
 * @param module_argc 请求参数数量
 * @param module_argv 请求参数数组
 * @return 处理结果，0表示成功，非0表示失败
 */
int handle_fpga_request(update_cgroups_parameters_function func,
   const char* module_name, int module_argc, char** module_argv);

/**
 * 从文件系统重新加载FPGA配置，对外暴露用于测试
 */
void reload_fpga_configuration();

#endif