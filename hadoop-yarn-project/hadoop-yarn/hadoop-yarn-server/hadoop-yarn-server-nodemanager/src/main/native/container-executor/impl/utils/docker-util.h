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
 * @file docker-util.h
 * @brief YARN NodeManager 容器执行器 Docker 工具头文件
 * @details 提供构建 Docker 命令行、解析配置、错误处理等工具能力，
 *          用于在YARN集群上支持Docker容器运行。
 */

#ifndef __YARN_POSIX_CONTAINER_EXECUTOR_DOCKER_UTIL_H__
#define __YARN_POSIX_CONTAINER_EXECUTOR_DOCKER_UTIL_H__

#include "configuration.h"

// Docker配置段名称
#define CONTAINER_EXECUTOR_CFG_DOCKER_SECTION "docker"
// Docker可执行文件路径配置键
#define DOCKER_BINARY_KEY "docker.binary"
// Docker inspect最大重试次数配置键
#define DOCKER_INSPECT_MAX_RETRIES_KEY "docker.inspect.max.retries"
// Docker命令执行配置段名称
#define DOCKER_COMMAND_FILE_SECTION "docker-command-execution"
// Docker inspect命令名称
#define DOCKER_INSPECT_COMMAND "inspect"
// Docker load命令名称
#define DOCKER_LOAD_COMMAND "load"
// Docker pull命令名称
#define DOCKER_PULL_COMMAND "pull"
// Docker rm命令名称
#define DOCKER_RM_COMMAND "rm"
// Docker run命令名称
#define DOCKER_RUN_COMMAND "run"
// Docker stop命令名称
#define DOCKER_STOP_COMMAND "stop"
// Docker kill命令名称
#define DOCKER_KILL_COMMAND "kill"
// Docker volume命令名称
#define DOCKER_VOLUME_COMMAND "volume"
// Docker start命令名称
#define DOCKER_START_COMMAND "start"
// Docker exec命令名称
#define DOCKER_EXEC_COMMAND "exec"
// Docker images命令名称
#define DOCKER_IMAGES_COMMAND "images"
// Docker服务模式启用配置键
#define DOCKER_SERVICE_MODE_ENABLED_KEY "docker.service-mode.enabled"
// Docker命令最大参数个数
#define DOCKER_ARG_MAX 1024
// 参数数组初始值
#define ARGS_INITIAL_VALUE { 0 };

/**
 * @brief Docker命令参数结构体
 * @details 存储构建好的Docker命令行参数数组，用于后续execv执行
 */
typedef struct args {
    // 当前已使用的参数个数
    int length;
    // 参数指针数组，最后一个元素为NULL
    char *data[DOCKER_ARG_MAX];
} args;

/**
 * 获取Docker可执行文件的完整路径
 * @param conf 容器执行器配置对象
 * @return Docker可执行路径字符串，需要由调用者释放内存
 */
char *get_docker_binary(const struct configuration *conf);

/**
 * 根据参数文件构建完整Docker命令行
 * @param command_file 存储Docker命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 检查是否设置了use-entry-point标志位
 * @return 0表示标志位已设置，非0表示未设置
 */
int get_use_entry_point_flag();

/**
 * 构建Docker inspect命令行，验证参数文件是否对应inspect命令
 * @param command_file 存储inspect命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_inspect_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 构建Docker load命令行，验证参数文件是否对应load命令
 * @param command_file 存储load命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_load_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 构建Docker pull命令行，验证参数文件是否对应pull命令
 * @param command_file 存储pull命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_pull_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 构建Docker rm命令行，验证参数文件是否对应rm命令
 * @param command_file 存储rm命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_rm_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 构建Docker run命令行，验证参数文件是否对应run命令
 * @param command_file 存储run命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_run_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 构建Docker stop命令行，验证参数文件是否对应stop命令
 * @param command_file 存储stop命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_stop_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 构建Docker kill命令行，验证参数文件是否对应kill命令
 * @param command_file 存储kill命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_kill_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 构建Docker volume命令行，验证参数文件是否对应volume命令
 * @param command_file 存储volume命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_volume_command(const char *command_file, const struct configuration *conf, args *args);

/**
 * 构建Docker start命令行，验证参数文件是否对应start命令
 * @param command_file 存储start命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_start_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 构建Docker exec命令行，验证参数文件是否对应exec命令
 * @param command_file 存储exec命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_exec_command(const char* command_file, const struct configuration* conf, args *args);

/**
 * 根据错误码获取对应错误信息字符串
 * @param error_code 错误码
 * @return 错误信息字符串常量
 */
const char *get_docker_error_message(const int error_code);

/**
 * 检查配置中是否启用Docker模块
 * @param conf 容器执行器配置对象
 * @return 1表示启用，0表示未启用
 */
int docker_module_enabled(const struct configuration *conf);

/**
 * 重置args参数结构体，清空所有参数
 * @param args 指向args结构体的指针
 */
void reset_args(args *args);

/**
 * 从args结构体提取可用于execv执行的参数数组
 * @param args 指向args结构体的指针
 * @return 符合execv要求的参数数组
 */
char** extract_execv_args(args *args);

/**
 * 获取docker inspect命令的最大重试次数配置
 * @param conf 容器执行器配置对象
 * @return 最大重试次数值
 */
int get_max_retries(const struct configuration *conf);

/**
 * 构建Docker images命令行，验证参数文件是否对应images命令
 * @param command_file 存储images命令参数的文件路径
 * @param conf 容器执行器配置对象
 * @param args 用于存储构建结果的参数缓冲区
 * @return 0表示成功，非0表示错误码
 */
int get_docker_images_command(const char* command_file, const struct configuration* conf, args *args);

#endif