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
 * runC 容器启动命令数据结构定义与操作接口
 * 用于YARN NodeManager使用runC启动容器时，解析和管理启动配置
 */
#ifndef RUNC_RUNC_LAUNCH_CMD_H
#define RUNC_RUNC_LAUNCH_CMD_H

#include "utils/cJSON/cJSON.h"

// 注意：修改此结构体后需要同步更新free_runc_launch_cmd中的内存释放逻辑
/** OCI镜像层配置结构体 */
typedef struct runc_launch_cmd_layer_spec {
  char* media_type; // 层数据的MIME类型
  char* path;       // 层数据在本地文件系统的路径
} rlc_layer_spec;

// 注意：修改此结构体后需要同步更新free_runc_launch_cmd中的内存释放逻辑
/** runC容器进程配置结构体 */
typedef struct runc_config_process_struct {
  cJSON* args;       // 要执行的命令及参数数组
  cJSON* cwd;        // 容器工作目录
  cJSON* env;        // 进程环境变量数组
} runc_config_process;

// 注意：修改此结构体后需要同步更新free_runc_launch_cmd中的内存释放逻辑
/** runC容器整体配置结构体 */
typedef struct runc_config_struct {
  cJSON* hostname;             // 容器内主机名
  cJSON* linux_config;         // runC配置中的Linux相关配置段
  cJSON* mounts;               // 容器绑定挂载配置
  runc_config_process process; // 容器进程配置
} runc_config;

// 注意：修改此结构体后需要同步更新free_runc_launch_cmd中的内存释放逻辑
/** runC容器启动命令完整配置结构体 */
typedef struct runc_launch_cmd_struct {
  char* run_as_user;          // 执行启动操作用户名
  char* username;             // 容器运行用户名
  char* app_id;               // YARN应用ID
  char* container_id;         // YARN容器ID
  char* pid_file;             // 要创建的PID文件路径
  char* script_path;          // 容器启动脚本路径
  char* cred_path;            // 容器凭证文件路径
  int https;                  // HTTPS是否启用标志
  char* keystore_path;        // 密钥库文件路径
  char* truststore_path;      // 信任库文件路径
  char** local_dirs;          // 以NULL结尾的本地目录数组
  char** log_dirs;            // 以NULL结尾的日志目录数组
  rlc_layer_spec* layers;     // OCI镜像层配置数组
  unsigned int num_layers;    // 镜像层数量
  int num_reap_layers_keep;   // 需要保留的层挂载数量
  runc_config config;         // runC容器配置
} runc_launch_cmd;


/**
 * 释放runC启动命令结构体及其所有关联内存
 */
void free_runc_launch_cmd(runc_launch_cmd* rlc);


/**
 * 验证runC容器启动命令配置是否合法
 * 验证通过返回true，否则返回false
 */
bool is_valid_runc_launch_cmd(const runc_launch_cmd* rlc);

/**
 * 从文件读取、解析并验证runC容器启动命令配置
 *
 * 解析成功返回启动命令结构体指针，失败返回NULL
 */
runc_launch_cmd* parse_runc_launch_cmd(const char* command_filename);

#endif /* RUNC_RUNC_LAUNCH_CMD_H */