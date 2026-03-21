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
 * @file container-executor.c
 * @brief YARN NodeManager 容器执行器核心实现，以setuid root权限运行，负责安全启动、管理和清理用户容器
 *
 * 该文件是YARN容器执行器的主实现，核心职责：
 * 1. 安全用户权限管理：以root权限验证用户身份后切换到对应用户启动容器
 * 2. 容器目录准备：创建用户、应用、容器工作目录并设置正确权限
 * 3. 容器生命周期管理：启动容器、写入pid、等待退出、记录退出码
 * 4. Docker/Runc容器支持：支持启动Docker和runc容器
 * 5. Cgroup资源管理：将容器pid加入对应cgroup实现资源限制
 * 6. 资源清理：递归删除容器工作目录清理残留资源
 * 7. 网络流量控制：调用tc命令进行网络流量管控
 */

#include "configuration.h"
#include "container-executor.h"
#include "utils/docker-util.h"
#include "utils/path-utils.h"
#include "utils/string-utils.h"
#include "runc/runc.h"
#include "util.h"
#include "config.h"

#include <inttypes.h>
#include <libgen.h>
#include <dirent.h>
#include <fcntl.h>
#ifdef __sun
#include <sys/param.h>
#define NAME_MAX MAXNAMELEN
#endif
#include <errno.h>
#include <grp.h>
#include <unistd.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/wait.h>
#include <getopt.h>
#include <sys/param.h>
#include <termios.h>
#ifdef __linux
#include <sys/vfs.h>
#include <linux/magic.h>
#endif

#ifndef HAVE_FCHMODAT
#include "compat/fchmodat.h"
#endif

#ifndef HAVE_FDOPENDIR
#include "compat/fdopendir.h"
#endif

#ifndef HAVE_FSTATAT
#include "compat/fstatat.h"
#endif

#ifndef HAVE_OPENAT
#include "compat/openat.h"
#endif

#ifndef HAVE_UNLINKAT
#include "compat/unlinkat.h"
#endif

// 如果未定义cgroup2超级块魔数则定义
#ifndef CGROUP2_SUPER_MAGIC
#define CGROUP2_SUPER_MAGIC 0x63677270
#endif

// 默认最小允许用户ID，低于此ID的系统用户禁止运行容器
static const int DEFAULT_MIN_USERID = 1000;

// 默认禁用用户列表，这些系统用户禁止运行容器
static const char* DEFAULT_BANNED_USERS[] = {"yarn", "mapred", "hdfs", "bin", 0};

// 功能开关默认值
static const int DEFAULT_TERMINAL_SUPPORT_ENABLED = 0;
static const int DEFAULT_DOCKER_SUPPORT_ENABLED = 0;
static const int DEFAULT_TC_SUPPORT_ENABLED = 0;
static const int DEFAULT_MOUNT_CGROUP_SUPPORT_ENABLED = 0;
static const int DEFAULT_YARN_SYSFS_SUPPORT_ENABLED = 0;
static const int DEFAULT_RUNC_SUPPORT_ENABLED = 0;

// /proc文件系统路径
static const char* PROC_PATH = "/proc";

// 流量控制(tc)二进制文件路径
static const char* TC_BIN = "/sbin/tc";
// tc修改状态默认选项
static const char* TC_MODIFY_STATE_OPTS [] = { "-b" , NULL};
// tc读取状态默认选项
static const char* TC_READ_STATE_OPTS [] = { "-b", NULL};
// tc读取统计信息默认选项
static const char* TC_READ_STATS_OPTS [] = { "-s",  "-b", NULL};

// 存储当前容器对应用户信息
struct passwd *user_detail = NULL;

// 日志文件指针
FILE* LOGFILE = NULL;
FILE* ERRORFILE = NULL;

// NodeManager运行的用户ID和组ID
static uid_t nm_uid = -1;
static gid_t nm_gid = -1;

// 全局配置对象
struct configuration CFG = {.size=0, .sections=NULL};
// 执行器配置段
struct section executor_cfg = {.size=0, .kv_pairs=NULL};

// 选中的容器日志目录
static char *chosen_container_log_dir = NULL;

char *concatenate(char *concat_pattern, char *return_path_name,
   int numArgs, ...);

// 设置NodeManager的uid和gid
void set_nm_uid(uid_t user, gid_t group) {
  nm_uid = user;
  nm_gid = group;
}

/**
 * 从安全配置文件加载容器执行器配置
 */
void read_executor_config(const char *file_name) {
  const struct section *tmp = NULL;
  int ret = read_config(file_name, &CFG);
  if (ret == 0) {
    tmp = get_configuration_section("", &CFG);
    if (tmp != NULL) {
      executor_cfg = *tmp;
    }
  }
}

/**
 * 释放执行器配置占用的内存
 */
void free_executor_configurations() {
    free_configuration(&CFG);
}

/**
 * 从配置中查找NodeManager所属用户组
 */
char *get_nodemanager_group() {
    return get_section_value(NM_GROUP_KEY, &executor_cfg);
}

/**
 * 检查容器执行器二进制文件权限是否符合安全要求
 * 要求：root拥有、NodeManager组、其他用户无写执行权限、启用setuid
 * @return 0 检查通过，-1 检查失败
 */
int check_executor_permissions(char *executable_file) {

  errno = 0;
#ifdef HAVE_CANONICALIZE_FILE_NAME
  char * resolved_path = canonicalize_file_name(executable_file);
#else
  char * resolved_path = realpath(executable_file, NULL);
#endif
  if (resolved_path == NULL) {
    fprintf(ERRORFILE,
        "Error resolving the canonical name for the executable : %s!",
        strerror(errno));
    return -1;
  }

  struct stat filestat;
  errno = 0;
  if (stat(resolved_path, &filestat) != 0) {
    fprintf(ERRORFILE,
            "Could not stat the executable : %s!.\n", strerror(errno));
    return -1;
  }

  uid_t binary_euid = filestat.st_uid;
  gid_t binary_gid = filestat.st_gid;

  // 二进制必须由root拥有
  if (binary_euid != 0) {
    fprintf(LOGFILE,
        "The container-executor binary should be user-owned by root.\n");
    return -1;
  }

  // 二进制必须属于配置的NodeManager用户组
  if (binary_gid != getgid()) {
    fprintf(LOGFILE, "The configured nodemanager group %d is different from"
            " the group of the executable %d\n", getgid(), binary_gid);
    return -1;
  }

  // 检查其他用户没有写和执行权限，防止恶意修改
  if ((filestat.st_mode & S_IWOTH) == S_IWOTH ||
      (filestat.st_mode & S_IXOTH) == S_IXOTH) {
    fprintf(LOGFILE,
            "The container-executor binary should not have write or execute "
            "for others.\n");
    return -1;
  }

  // 必须启用setuid位，才能以root权限运行
  if ((filestat.st_mode & S_ISUID) == 0) {
    fprintf(LOGFILE, "The container-executor binary should be set setuid.\n");
    return -1;
  }

  return 0;
}

/**
 * 切换有效用户ID，用于临时切换权限执行特权操作
 * @param user 目标用户ID
 * @param group 目标组ID
 * @return 0 成功，-1 失败
 */
int change_effective_user(uid_t user, gid_t group) {
  if (geteuid() == user) {
    return 0;
  }
  // 需要先切回root才能切换到其他用户
  if (seteuid(0) != 0) {
    return -1;
  }
  if (setegid(group) != 0) {
    fprintf(LOGFILE, "Failed to set effective group id %d - %s\n", group,
            strerror(errno));
    return -1;
  }
  if (seteuid(user) != 0) {
    fprintf(LOGFILE, "Failed to set effective user id %d - %s\n", user,
            strerror(errno));
    return -1;
  }
  return 0;
}

/**
 * 切换有效用户到NodeManager用户
 * @return 0 成功，-1 失败
 */
int change_effective_user_to_nm() {
  return change_effective_user(nm_uid, nm_gid);
}

#ifdef __linux
/**
 * 以root权限将进程pid写入cgroup.procs文件
 * @param cgroup_file cgroup文件路径
 * @param pid 要写入的进程ID
 * @return 0 成功，-1 失败
 */
static int write_pid_to_cgroup_as_root(const char* cgroup_file, pid_t pid) {
  int rc = 0;
  // 保存当前用户信息，操作完成后切回
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(0, 0) != 0) {
    rc =  -1;
    goto cleanup;
  }

  // 检查目标文件所在文件系统是否为cgroup/cgroup2
  struct statfs buf;
  if (statfs(cgroup_file, &buf) == -1) {
    fprintf(LOGFILE, "Can't statfs file %s as node manager - %s\n", cgroup_file,
           strerror(errno));
    rc = -1;
    goto cleanup;
  } else if (buf.f_type != CGROUP_SUPER_MAGIC && buf.f_type != CGROUP2_SUPER_MAGIC) {
    fprintf(LOGFILE, "Pid file %s is not located on cgroup/cgroup2 filesystem\n", cgroup_file);
    rc = -1;
    goto cleanup;
  }

  // 打开cgroup.procs文件
  int cgroup_fd = open(cgroup_file, O_WRONLY | O_APPEND, 0);
  if (cgroup_fd == -1) {
    fprintf(LOGFILE, "Can't open file %s as node manager - %s\n", cgroup_file,
           strerror(errno));
    rc = -1;
    goto cleanup;
  }

  // 转换pid为字符串写入
  char pid_buf[21];
  snprintf(pid_buf, sizeof(pid_buf), "%" PRId64, (int64_t)pid);
  ssize_t written = write(cgroup_fd, pid_buf, strlen(pid_buf));
  close(cgroup_fd);
  if (written == -1) {
    fprintf(LOGFILE, "Failed to write pid to file %s - %s\n",
       cgroup_file, strerror(errno));
    rc = -1;
    goto cleanup;
  }

cleanup:
  // 切回原调用用户
  if (change_effective_user(user, group)) {
    rc = -1;
  }

  return rc;
}
#endif

/**
 * 以NodeManager用户身份将pid写入pid文件
 * @param pid_file 目标pid文件路径
 * @param pid 要写入的进程ID
 * @return 0 成功，-1 失败
 */
static int write_pid_to_file_as_nm(const char* pid_file, pid_t pid) {
  int rc = 0;
  char *temp_pid_file = NULL;
  // 保存当前用户信息，操作完成后切回
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(nm_uid, nm_gid) != 0) {
    fprintf(ERRORFILE, "Could not change to effective users %d, %d\n", nm_uid, nm_gid);
    rc = -1;
    goto cleanup;
  }

  // 先写入临时文件，再原子重命名，保证不产生部分写入的pid文件
  temp_pid_file = concatenate("%s.tmp", "pid_file_path", 1, pid_file);
  fprintf(LOGFILE, "Writing to tmp file %s\n", temp_pid_file);
  // 权限设置为700，只有NodeManager可读写
  int pid_fd = open(temp_pid_file, O_WRONLY|O_CREAT|O_EXCL, S_IRWXU);
  if (pid_fd == -1) {
    fprintf(LOGFILE, "Can't open file %s as node manager - %s\n", temp_pid_file,
           strerror(errno));
    rc = -1;
    goto cleanup;
  }

  // 写入pid到临时文件
  char pid_buf[21];
  snprintf(pid_buf, 21, "%" PRId64, (int64_t)pid);
  ssize_t written = write(pid_fd, pid_buf, strlen(pid_buf));
  close(pid_fd);
  if (written == -1) {
    fprintf(LOGFILE, "Failed to write pid to file %s as node manager - %s\n",
       temp_pid_file, strerror(errno));
    rc = -1;
    goto cleanup;
  }

  // 原子重命名覆盖目标文件
  if (rename(temp_pid_file, pid_file)) {
    fprintf(LOGFILE, "Can't move pid file from %s to %s as node manager - %s\n",
        temp_pid_file, pid_file, strerror(errno));
    unlink(temp_pid_file);
    rc = -1;
    goto cleanup;
  }

cleanup:
  // 切回原用户
  if (change_effective_user(user, group)) {
    rc = -1;
  }

  free(temp_pid_file);
  return rc;
}

/**
 * 以NodeManager用户身份将容器退出码写入文件
 * @param exit_code_file 目标文件路径
 * @param exit_code 容器退出码
 * @return 0 成功，-1 失败
 */
static int write_exit_code_file_as_nm(const char* exit_code_file,
    int exit_code) {
  char *tmp_ecode_file = NULL;
  int rc = 0;
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(nm_uid, nm_gid) != 0) {
    fprintf(ERRORFILE, "Could not change to effective users %d, %d\n", nm_uid, nm_gid);
    rc = -1;
    goto cleanup;
  }
  tmp_ecode_file = concatenate("%s.tmp", "exit_code_path", 1,
      exit_code_file);
  if (tmp_ecode_file == NULL) {
    rc = -1;
    goto cleanup;
  }

  // 创建临时文件，权限700
  int ecode_fd = open(tmp_ecode_file, O_WRONLY|O_CREAT|O_EXCL, S_IRWXU);
  if (ecode_fd == -1) {
    fprintf(LOGFILE, "Can't open file %s - %s\n", tmp_ecode_file,
           strerror(errno));
    rc = -1;
    goto cleanup;
  }

  // 写入退出码
  char ecode_buf[21];
  snprintf(ecode_buf, sizeof(ecode_buf), "%d", exit_code);
  ssize_t written = write(ecode_fd, ecode_buf, strlen(ecode_buf));
  close(ecode_fd);
  if (written == -1) {
    fprintf(LOGFILE, "Failed to write exit code to file %s - %s\n",
       tmp_ecode_file, strerror(errno));
    rc = -1;
    goto cleanup;
  }

  // 原子重命名
  if (rename(tmp_ecode_file, exit_code_file)) {
    fprintf(LOGFILE, "Can't move exit code file from %s to %s - %s\n",
        tmp_ecode_file, exit_code_file, strerror(errno));
    unlink(tmp_ecode_file);
    rc = -1;