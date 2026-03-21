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
 * GPU资源隔离模块头文件
 * 属于YARN NodeManager容器执行器，负责处理容器GPU资源分配与cgroups参数配置
 */

#ifdef __FreeBSD__
#define _WITH_GETLINE
#endif

#ifndef _MODULES_GPU_GPU_MUDULE_H_
#define _MODULES_GPU_GPU_MUDULE_H_

/** GPU设备主设备号配置项键名 */
#define GPU_MAJOR_NUMBER_CONFIG_KEY "gpu.major-device-number"
/** 允许使用的GPU设备次设备号列表配置项键名 */
#define GPU_ALLOWED_DEVICES_MINOR_NUMBERS "gpu.allowed-device-minor-numbers"
/** GPU模块配置段名称 */
#define GPU_MODULE_SECTION_NAME "gpu"

// 函数指针类型定义，用于单元测试桩注入
typedef int (*update_cgroups_parameters_func)(const char*, const char*,
   const char*, const char*);

/**
 * 处理容器GPU资源分配请求，配置对应cgroups参数
 * @param func 更新cgroups参数的回调函数指针
 * @param module_name 模块名称
 * @param module_argc 模块参数个数
 * @param module_argv 模块参数数组
 * @return 处理结果，0成功非0失败
 */
int handle_gpu_request(update_cgroups_parameters_func func,
   const char* module_name, int module_argc, char** module_argv);

/**
 * 从文件系统重新加载GPU模块配置，导出用于测试
 */
void reload_gpu_configuration();

#endif