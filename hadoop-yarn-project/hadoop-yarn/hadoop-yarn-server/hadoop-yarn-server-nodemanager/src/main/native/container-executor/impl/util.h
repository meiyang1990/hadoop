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
 * @file util.h
 * @brief YARN NodeManager 容器执行器公共工具函数头文件
 * 提供字符串处理、内存管理、错误码定义等通用能力，供容器执行器各个模块调用
 */

#ifndef __YARN_POSIX_CONTAINER_EXECUTOR_UTIL_H__
#define __YARN_POSIX_CONTAINER_EXECUTOR_UTIL_H__

/** 定义平台无关的最大路径长度，固定为4KB，替代系统定义的PATH_MAX */
#define EXECUTOR_PATH_MAX 4096

#include <stdio.h>
#include <stdlib.h>

/** 容器执行器错误码枚举，定义了所有可能的错误类型和对应的错误编号 */
enum errorcodes {
  INVALID_ARGUMENT_NUMBER = 1,          // 输入参数个数错误
  //INVALID_USER_NAME 2
  INVALID_COMMAND_PROVIDED = 3,         // 提供的执行命令无效
  // SUPER_USER_NOT_ALLOWED_TO_RUN_TASKS (NOT USED) 4
  INVALID_NM_ROOT_DIRS = 5,              // NodeManager根目录配置无效
  SETUID_OPER_FAILED = 6,                // setuid操作失败，无法切换用户
  UNABLE_TO_EXECUTE_CONTAINER_SCRIPT = 7, // 无法执行容器启动脚本
  UNABLE_TO_SIGNAL_CONTAINER = 8,        // 无法向容器发送信号
  INVALID_CONTAINER_PID = 9,             // 容器PID无效
  // ERROR_RESOLVING_FILE_PATH (NOT_USED) 10
  // RELATIVE_PATH_COMPONENTS_IN_FILE_PATH (NOT USED) 11
  // UNABLE_TO_STAT_FILE (NOT USED) 12
  // FILE_NOT_OWNED_BY_ROOT (NOT USED) 13
  // PREPARE_CONTAINER_DIRECTORIES_FAILED (NOT USED) 14
  // INITIALIZE_CONTAINER_FAILED (NOT USED) 15
  // PREPARE_CONTAINER_LOGS_FAILED (NOT USED) 16
  // INVALID_LOG_DIR (NOT USED) 17
  OUT_OF_MEMORY = 18,                    // 内存分配失败
  // INITIALIZE_DISTCACHEFILE_FAILED (NOT USED) 19
  INITIALIZE_USER_FAILED = 20,           // 用户环境初始化失败
  PATH_TO_DELETE_IS_NULL = 21,           // 待删除路径为空指针
  INVALID_CONTAINER_EXEC_PERMISSIONS = 22, // 容器执行器权限不符合要求
  // PREPARE_JOB_LOGS_FAILED (NOT USED) 23
  INVALID_CONFIG_FILE = 24,              // 配置文件无效
  SETSID_OPER_FAILED = 25,               // setsid操作失败，无法创建新会话
  WRITE_PIDFILE_FAILED = 26,              // 写入PID文件失败
  WRITE_CGROUP_FAILED = 27,               // 写入cgroup配置失败
  TRAFFIC_CONTROL_EXECUTION_FAILED = 28, // 流量控制命令执行失败
  DOCKER_RUN_FAILED = 29,                // Docker容器启动失败
  ERROR_OPENING_DOCKER_FILE = 30,        // 打开Docker相关文件失败
  ERROR_READING_DOCKER_FILE = 31,        // 读取Docker相关文件失败
  FEATURE_DISABLED = 32,                 // 请求功能已被禁用
  COULD_NOT_CREATE_SCRIPT_COPY = 33,      // 无法创建脚本副本
  COULD_NOT_CREATE_CREDENTIALS_COPY = 34, // 无法创建凭据文件副本
  COULD_NOT_CREATE_WORK_DIRECTORIES = 35, // 无法创建工作目录
  COULD_NOT_CREATE_APP_LOG_DIRECTORIES = 36, // 无法创建应用日志目录
  COULD_NOT_CREATE_TMP_DIRECTORIES = 37,  // 无法创建临时目录
  ERROR_CREATE_CONTAINER_DIRECTORIES_ARGUMENTS = 38, // 创建容器目录参数错误
  ERROR_SANITIZING_DOCKER_COMMAND = 39,   // Docker命令安全清理失败
  DOCKER_IMAGE_INVALID = 40,              // Docker镜像名称无效
  // DOCKER_CONTAINER_NAME_INVALID = 41, (NOT USED)
  ERROR_COMPILING_REGEX = 42,            // 正则表达式编译失败
  INVALID_CONTAINER_ID = 43,             // 容器ID无效
  DOCKER_EXEC_FAILED = 44,               // Docker exec命令执行失败
  COULD_NOT_CREATE_KEYSTORE_COPY = 45,    // 无法创建密钥库副本
  COULD_NOT_CREATE_TRUSTSTORE_COPY = 46,  // 无法创建信任库副本
  ERROR_CALLING_SETVBUF = 47,             // setvbuf调用失败
  BUFFER_TOO_SMALL = 48,                 // 缓冲区空间不足
  INVALID_MOUNT = 49,                     // 挂载配置无效
  INVALID_RO_MOUNT = 50,                 // 只读挂载配置无效
  INVALID_RW_MOUNT = 51,                 // 读写挂载配置无效
  MOUNT_ACCESS_ERROR = 52,                // 挂载访问权限错误
  INVALID_DOCKER_COMMAND_FILE = 53,      // Docker命令文件无效
  INCORRECT_DOCKER_COMMAND = 54,          // Docker命令格式错误
  INVALID_DOCKER_CONTAINER_NAME = 55,     // Docker容器名称无效
  INVALID_DOCKER_IMAGE_NAME = 56,        // Docker镜像名称无效
  INVALID_DOCKER_USER_NAME = 57,         // Docker用户名无效
  INVALID_DOCKER_INSPECT_FORMAT = 58,     // Docker inspect格式无效
  UNKNOWN_DOCKER_COMMAND = 59,            // 未知Docker命令
  INVALID_DOCKER_NETWORK = 60,            // Docker网络配置无效
  INVALID_DOCKER_PORTS_MAPPING = 61,     // Docker端口映射配置无效
  INVALID_DOCKER_CAPABILITY = 62,         // Docker能力配置无效
  PRIVILEGED_DOCKER_CONTAINERS_DISABLED = 63, // 特权Docker容器已禁用
  INVALID_DOCKER_DEVICE = 64,             // Docker设备配置无效
  INVALID_DOCKER_STOP_COMMAND = 65,      // Docker停止命令配置无效
  INVALID_DOCKER_KILL_COMMAND = 66,       // Docker kill命令配置无效
  INVALID_DOCKER_VOLUME_DRIVER = 67,      // Docker卷驱动配置无效
  INVALID_DOCKER_VOLUME_NAME = 68,        // Docker卷名称无效
  INVALID_DOCKER_VOLUME_COMMAND = 69,     // Docker卷命令配置无效
  DOCKER_PID_HOST_DISABLED = 70,          // 主机PID命名空间模式已禁用
  INVALID_DOCKER_PID_NAMESPACE = 71,      // Docker PID命名空间配置无效
  INVALID_DOCKER_IMAGE_TRUST = 72,        // Docker镜像信任验证失败
  INVALID_DOCKER_TMPFS_MOUNT = 73,        // Docker tmpfs挂载配置无效
  INVALID_DOCKER_RUNTIME = 74,            // Docker运行时配置无效
  DOCKER_SERVICE_MODE_DISABLED = 75,      // Docker服务模式已禁用
  ERROR_RUNC_SETUP_FAILED = 76,           // runc容器初始化失败
  ERROR_RUNC_RUN_FAILED = 77,             // runc容器启动失败
  ERROR_RUNC_REAP_LAYER_MOUNTS_FAILED = 78, // runc层挂载回收失败
  ERROR_DOCKER_CONTAINER_EXEC_FAILED = 79, // Docker容器执行命令失败
  CANNOT_GET_EXECUTABLE_NAME_FROM_READLINK = 80, // 无法通过readlink获取可执行文件路径
  TOO_LONG_EXECUTOR_PATH = 81,            // 容器执行器路径超过最大长度限制
  CANNOT_GET_EXECUTABLE_NAME_FROM_KERNEL = 82, // 无法从内核获取可执行文件路径
  CANNOT_GET_EXECUTABLE_NAME_FROM_PID = 83, // 无法通过PID获取可执行文件路径
  WRONG_PATH_OF_EXECUTABLE = 84           // 可执行文件路径与预期不符
};

/* 取最小值宏定义 */
#ifndef MIN
#define MIN(a,b) (((a)<(b)?(a):(b))
#endif /* MIN */
/* 取最大值宏定义 */
#ifndef MAX
#define MAX(a,b) (((a)>(b)?(a):(b))
#endif  /* MAX */

// 普通日志输出文件指针，全局变量
extern FILE *LOGFILE;
// 错误日志输出文件指针，全局变量
extern FILE *ERRORFILE;

/**
 * 使用'%'作为分隔符拆分字符串，调用者负责释放返回数组的内存，使用free_values释放
 * @param str 待拆分的字符串
 * @return 拆分后的字符串数组
 */
char** split(char *str);

/**
 * 使用指定分隔符拆分字符串，调用者负责释放返回数组的内存，使用free_values释放
 * @param value 待拆分的字符串
 * @param delimiter 分隔符
 * @return 拆分后的字符串数组
 */
char** split_delimiter(char *value, const char *delimiter);

/**
 * 释放字符串数组占用的内存
 * @param values 待释放的字符串数组
 */
void free_values(char **values);

/**
 * 去除字符串首尾空白字符，返回结果需要调用者释放内存
 * @param input 输入原始字符串
 * @return 修剪后的字符串，通过malloc分配，需调用者释放
*/
char* trim(const char *input);

/**
 * 使用正则表达式匹配输入字符串
 * @param regex_str 正则表达式
 * @param input 待匹配输入字符串
 * @return 匹配成功返回0，不匹配返回非0
 */
int execute_regex_match(const char *regex_str, const char *input);

/**
 * 转义字符串中的单引号，用于将字符串放入bash命令中单引号包裹的参数中，供bash命令执行时正确解析
 * @param str 需要转义的原始字符串
 * @return 转义后的字符串，调用者负责释放内存
 */
char* escape_single_quote(const char *str);

/**
 * 将参数格式化为'param="arg"'格式，并追加到目标缓冲区，缓冲区不足时自动扩容
 * @param str 目标缓冲区指针的指针，缓冲区会自动扩容
 * @param size 当前缓冲区大小指针
 * @param param 参数名称
 * @param arg 参数值
 */
void quote_and_append_arg(char **str, size_t *size, const char* param, const char *arg);
// 缓冲区扩容步长，每次需要扩容时增加的字节数
#define QUOTE_AND_APPEND_ARG_GROWTH (1024)

/**
 * 分配并清零一块内存，分配失败直接退出程序并返回内存不足错误码
 * @param num 分配元素个数
 * @param size 每个元素大小
 * @return 分配好的内存指针，调用者负责释放
 */
inline void* alloc_and_clear_memory(size_t num, size_t size) {
  void *ret = calloc(num, size);
  if (ret == NULL) {
    printf("Could not allocate memory, exiting\n");
    exit(OUT_OF_MEMORY);
  }
  return ret;
}

/**
 * 根据错误码获取对应的错误描述字符串
 * @param error_code 错误码
 * @return 错误描述字符串常量
 */
const char *get_error_message(const int error_code);

/**
 * 检查字符串是否是正则表达式
 * @param str 输入字符串
 * @return 1表示是正则表达式，0表示不是
 */
int is_regex(const char *str);

#endif