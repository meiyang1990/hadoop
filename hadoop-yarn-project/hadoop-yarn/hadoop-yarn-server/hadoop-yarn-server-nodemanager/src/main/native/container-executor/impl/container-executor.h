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
 * YARN NodeManager 容器执行器头文件
 * 核心职责：定义容器执行器的常量、枚举和函数声明，负责以指定用户权限启动/管理YARN容器
 * 实现Linux平台下安全的容器权限隔离，支持cgroups、Docker、runc等多种运行环境
 */

/* FreeBSD protects the getline() prototype. See getline(3) for more */
#ifdef __FreeBSD__
// FreeBSD平台需要定义该宏才能启用getline函数
#define _WITH_GETLINE
#endif

#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>

// 外部可执行命令类型枚举
enum command {
  INITIALIZE_CONTAINER = 0,
  LAUNCH_CONTAINER = 1,
  SIGNAL_CONTAINER = 2,
  DELETE_AS_USER = 3,
  LAUNCH_DOCKER_CONTAINER = 4,
  LIST_AS_USER = 5,
  SYNC_YARN_SYSFS = 6
};

// 内部操作类型枚举，对应不同的处理逻辑
enum operations {
  CHECK_SETUP = 1,
  MOUNT_CGROUPS = 2,
  TRAFFIC_CONTROL_MODIFY_STATE = 3,
  TRAFFIC_CONTROL_READ_STATE = 4,
  TRAFFIC_CONTROL_READ_STATS = 5,
  RUN_AS_USER_INITIALIZE_CONTAINER = 6,
  RUN_AS_USER_LAUNCH_CONTAINER = 7,
  RUN_AS_USER_SIGNAL_CONTAINER = 8,
  RUN_AS_USER_DELETE = 9,
  RUN_AS_USER_LAUNCH_DOCKER_CONTAINER = 10,
  RUN_DOCKER = 11,
  RUN_AS_USER_LIST = 12,
  REMOVE_DOCKER_CONTAINER = 13,
  INSPECT_DOCKER_CONTAINER = 14,
  RUN_AS_USER_SYNC_YARN_SYSFS = 15,
  EXEC_CONTAINER = 16,
  RUN_RUNC_CONTAINER = 17,
  REAP_RUNC_LAYER_MOUNTS = 18
};

// NodeManager容器执行器配置组键名
#define NM_GROUP_KEY "yarn.nodemanager.linux-container-executor.group"
// 用户缓存目录路径模板
#define USER_DIR_PATTERN "%s/usercache/%s"
// 用户文件缓存目录路径模板
#define USER_FILECACHE_DIR_PATTERN "%s/usercache/%s/filecache"
// 应用缓存目录路径模板
#define NM_APP_DIR_PATTERN USER_DIR_PATTERN "/appcache/%s"
// 容器工作目录路径模板
#define CONTAINER_DIR_PATTERN NM_APP_DIR_PATTERN "/%s"
// 容器启动脚本文件名
#define CONTAINER_SCRIPT "launch_container.sh"
// 容器令牌凭证文件名
#define CREDENTIALS_FILENAME "container_tokens"
// YARN提供的密钥库文件名
#define KEYSTORE_FILENAME "yarn_provided.keystore"
// YARN提供的信任库文件名
#define TRUSTSTORE_FILENAME "yarn_provided.truststore"
// 最小用户ID配置键名
#define MIN_USERID_KEY "min.user.id"
// 禁止运行容器的用户列表配置键名
#define BANNED_USERS_KEY "banned.users"
// 允许运行容器的系统用户列表配置键名
#define ALLOWED_SYSTEM_USERS_KEY "allowed.system.users"
// 终端功能开启配置键名
#define TERMINAL_SUPPORT_ENABLED_KEY "feature.terminal.enabled"
// Docker支持开启配置键名
#define DOCKER_SUPPORT_ENABLED_KEY "feature.docker.enabled"
// 流量控制(tc)支持开启配置键名
#define TC_SUPPORT_ENABLED_KEY "feature.tc.enabled"
// Cgroup挂载支持开启配置键名
#define MOUNT_CGROUP_SUPPORT_ENABLED_KEY "feature.mount-cgroup.enabled"
// YARN SysFS支持开启配置键名
#define YARN_SYSFS_SUPPORT_ENABLED_KEY "feature.yarn.sysfs.enabled"
// runc支持开启配置键名
#define RUNC_SUPPORT_ENABLED_KEY "feature.runc.enabled"
// 临时目录名
#define TMP_DIR "tmp"
// /tmp目录映射名
#define ROOT_TMP_DIR "private_slash_tmp"
// /var/tmp目录映射名
#define ROOT_VAR_TMP_DIR "private_var_slash_tmp"
// 命令执行配置段名称
#define COMMAND_FILE_SECTION "command-execution"

// 当前操作用户的密码信息结构体
extern struct passwd *user_detail;
// 执行器配置段结构体
extern struct section executor_cfg;

/**
 * 从安全配置文件加载容器执行器配置
 * @param file_name 安全配置文件路径
 */
void read_executor_config(const char* file_name);

/**
 * 从容器执行器配置中查找NodeManager用户组
 * @return NodeManager用户组名称，失败返回NULL
 */
char *get_nodemanager_group();

/**
 * 检查容器执行器二进制文件的权限，确保安全合规
 * 检查项：
 *    * 所有者必须是root
 *    * 所属组必须是配置的指定用户组
 *    * 其他用户必须没有任何权限
 *    * 必须设置setuid/setgid位
 * @param executable_file 待检查的二进制文件路径
 * @return -1 检查失败，0 检查成功
 */
int check_executor_permissions(char *executable_file);

// 已存在注释，保留
void read_executor_config(const char* file_name);

/**
 * 释放执行器配置占用的内存
 */
void free_executor_configurations();

/**
 * 初始化容器应用目录结构
 * @param user 运行容器的用户名
 * @param app_id 应用ID
 * @param container_id 容器ID
 * @param credentials 凭证文件路径
 * @param local_dirs NodeManager本地目录列表
 * @param log_dirs NodeManager日志目录列表
 * @param args 额外参数
 * @return 0 成功，非0 失败
 */
int initialize_app(const char *user, const char *app_id,
                   const char *container_id,
                   const char *credentials, char* const* local_dirs,
                   char* const* log_dirs, char* const* args);

/**
 * 以指定用户身份启动Docker容器
 * @param user 运行容器的用户名
 * @param app_id 应用ID
 * @param container_id 容器ID
 * @param work_dir 容器工作目录
 * @param script_name 启动脚本名称
 * @param cred_file 凭证文件路径
 * @param https 是否启用HTTPS，1为启用，0为不启用
 * @param keystore_file 密钥库文件路径
 * @param truststore_file 信任库文件路径
 * @param pid_file PID文件路径，用于写入容器进程ID
 * @param local_dirs NodeManager本地目录列表
 * @param log_dirs NodeManager日志目录列表
 * @param command_file 命令文件路径
 * @return 失败返回错误码，成功不返回
 */
int launch_docker_container_as_user(const char * user, const char *app_id,
                              const char *container_id, const char *work_dir,
                              const char *script_name, const char *cred_file,
                              const int https,
                              const char *keystore_file, const char *truststore_file,
                              const char *pid_file, char* const* local_dirs,
                              char* const* log_dirs,
                              const char *command_file);

/*
 * 以指定用户身份启动容器，主要完成以下工作：
 * 1) 创建容器工作目录和日志目录，确保子进程可访问
 * 2) 将启动脚本从NodeManager目录复制到容器工作目录
 * 3) 配置容器运行环境变量
 * 4) 通过execlp替换当前进程镜像，启动容器进程
 * @param user 要切换到的用户名
 * @param app_id 应用ID
 * @param container_id 容器ID
 * @param work_dir 容器工作目录
 * @param script_name 启动容器的脚本名称
 * @param cred_file 需要复制到工作目录的凭证文件路径
 * @param https 1表示提供密钥库和信任库，0表示不提供
 * @param keystore_file 需要复制到工作目录的密钥库文件路径
 * @param truststore_file 需要复制到工作目录的信任库文件路径
 * @param pid_file 用于写入进程ID的文件路径
 * @param local_dirs NodeManager本地目录列表
 * @param log_dirs NodeManager日志目录列表
 * @param resources_key 资源强制隔离类型（none, cgroups）
 * @param resources_value 应用资源强制隔离所需参数
 * @return 错误返回错误码，成功不返回
 */
int launch_container_as_user(const char * user, const char *app_id,
                     const char *container_id, const char *work_dir,
                     const char *script_name, const char *cred_file,
                     const int https,
                     const char *keystore_file, const char *truststore_file,
                     const char *pid_file, char* const* local_dirs,
                     char* const* log_dirs, const char *resources_key,
                     char* const* resources_value);

/**
 * 向指定用户启动的容器发送信号
 * 会向PID对应的进程组发送信号
 * @param user 发送信号的对应用户
 * @param pid 目标进程ID
 * @param sig 要发送的信号编号
 * @return 错误返回错误码，0表示成功
 */
int signal_container_as_user(const char *user, int pid, int sig);

/**
 * 以指定用户身份递归删除目录或文件
 * 如果目录分布在多个磁盘卷上，baseDirs传入多个基础目录，会删除每个基础目录下对应的目标目录
 * 如果未指定baseDirs，则dir_to_be_deleted视为绝对路径
 * @param user 执行删除操作的用户名
 * @param dir_to_be_deleted 要删除的目标目录/文件
 * @param baseDirs 基础目录列表
 * @return 0 成功，非0 失败
 */
int delete_as_user(const char *user,
                   const char *dir_to_be_deleted,
                   char* const* baseDirs);

/**
 * 以对应用户身份列出指定目录的文件列表，输出到标准输出
 * target_dir始终视为绝对路径
 * @param target_dir 要列出文件的目标目录
 * @return 0 成功，非0 失败
 */
int list_as_user(const char *target_dir);

/**
 * 以NodeManager身份检查PID文件，要求文件不存在
 * @param filename 待检查的PID文件路径
 * @return 0 检查通过，非0 检查失败
 */
int check_pidfile_as_nm(const char* filename);

/**
 * 设置NodeManager的UID和GID
 * 用于在特权操作中切换当前进程的有效UID和GID
 * @param user NodeManager用户ID
 * @param group NodeManager用户组ID
 */
void set_nm_uid(uid_t user, gid_t group);

/**
 * 检查用户是否为合法的容器运行用户
 * 检查项：
 *   1. 不是root用户
 *   2. UID大于等于配置的最小UID
 *   3. 不在禁止用户列表中
 * @param user 待检查的用户名
 * @return 检查失败返回NULL，成功返回用户密码信息结构体
 */
struct passwd* check_user(const char *user);

/**
 * 切换当前进程用户到指定用户
 * @param user 目标用户名
 * @return 0 成功，非0 失败
 */
int set_user(const char *user);

/**
 * 拼接生成用户目录路径
 * @param nm_root NodeManager根目录
 * @param user 用户名
 * @return 拼接后的用户目录路径，需要调用者释放内存
 */
char *get_user_directory(const char *nm_root, const char *user);

/**
 * 拼接生成应用目录路径
 * @param nm_root NodeManager根目录
 * @param user 用户名
 * @param app_id 应用ID
 * @return 拼接后的应用目录路径，需要调用者释放内存
 */
char *get_app_directory(const char * nm_root, const char *user,
                        const char *app_id);

/**
 * 检查NodeManager本地目录权限是否合法
 * @param caller_uid 调用者用户ID
 * @param nm_root NodeManager根目录
 * @return 0 检查通过，非0 检查失败
 */
int check_nm_local_dir(uid_t caller_uid, const char *nm_root);

/**
 * 拼接生成容器工作目录路径
 * @param nm_root NodeManager根目录
 * @param user 用户名
 * @param app_id 应用ID
 * @param container_id 容器ID
 * @return 拼接后的容器工作目录路径，需要调用者释放内存
 */
char *get_container_work_directory(const char *nm_root, const char *user,
				 const char *app_id, const char *container_id);

/**
 * 拼接生成容器启动脚本路径
 * @param work_dir 容器工作目录
 * @return 拼接后的脚本路径，需要调用者释放内存
 */
char *get_container_launcher_file(const char* work_dir);

/**
 * 拼接生成容器凭证文件路径
 * @param work_dir 容器工作目录
 * @return 拼接后的凭证文件路径，需要调用者释放内存
 */
char *get_container_credentials_file(const char* work_dir);

/**
 * 拼接生成容器密钥库文件路径
 * @param work_dir 容器工作目录
 * @return 拼接后的密钥库文件路径，需要调用者释放内存
 */
char *get_container_keystore_file(const char* work_dir);

/**
 * 拼接生成容器信任库文件路径
 * @param work_dir 容器工作目录
 * @return 拼接后的信任库文件路径，需要调用者释放内存
 */
char *get_container_truststore_file(const char* work_dir);

/**
 * 拼接生成应用日志目录路径
 * @param log_root 日志根目录
 * @param appid 应用ID
 * @return 拼接后的应用日志目录路径，需要调用者释放内存
 */
char* get_app_log_directory(const char* log_root, const char* appid);

/**
 * 拼接生成容器日志目录路径
 * @param log_root 日志根目录
 * @param app_id 应用ID
 * @param container_id 容器ID
 * @return 拼接后的容器日志目录路径，需要调用者释放内存
 */
char* get_container_log_directory(const char *log_root, const char *app_id,
                                  const char *container_id);
/**
 * 递归创建目录，确保路径和所有父目录都按指定权限创建
 * @param path 要创建的目标路径
 * @param perm 目录权限
 * @return 0 成功，非0 失败
 */
int mkdirs(const char* path, mode_t perm);

/**
 * 初始化指定用户的目录结构
 * @param user 用户名
 * @param local_dirs NodeManager本地目录列表
 * @return 0 成功，非0 失败
 */
int initialize_user(const char *user, char* const* local_dirs);

/**
 * 为用户创建顶级目录，权限规则：
 * 父目录不可被用户写入，创建的目录权限为02700，所有者为对应用户，所属组为NodeManager用户组
 * @param path 要创建的目录路径
 * @return 非0 失败，0 成功
 */
int create_directory_for_user(const char* path);

/**
 * 切换当前进程的实际用户ID和有效用户ID
 * @param user 目标用户ID
 * @param group 目标用户组ID
 * @return 0 成功，非0 失败
 */
int change_user(uid_t user, gid_t group);

/**
 * 仅切换当前进程的有效用户ID
 * @param user 目标用户ID
 * @param group 目标用户组ID
 * @return 0 成功，非0 失败
 */
int change_effective_user(uid_t user, gid_t group);

/**
 * 切换有效用户ID回NodeManager
 * @return 0 成功，非0 失败
 */
int change_effective_user_to_nm();

/**
 * 挂载cgroup到指定层级
 * @param pair cgroup名称与挂载点对
 * @param hierarchy cgroup层级路径
 * @return 0 成功，非0 失败
 */
int mount_cgroup(const char *pair, const char *hierarchy);

/**
 * 检查目录权限是否符合要求
 * @param npath 待检查目录路径
 * @param st_mode 当前目录权限位
 * @param desired 期望权限位
 * @param finalComponent 是否为路径最后一级目录
 * @return 0 检查通过，非0 检查失败
 */
int check_dir(const char* npath, mode_t st_mode, mode_t desired,
   int finalComponent);

/**
 * 创建并验证