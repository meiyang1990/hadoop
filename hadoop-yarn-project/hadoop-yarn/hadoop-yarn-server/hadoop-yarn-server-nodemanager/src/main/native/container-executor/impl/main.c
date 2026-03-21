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
 * @file main.c
 * @brief YARN NodeManager 容器执行器主程序
 *
 * 负责以指定用户身份安全启动、管理YARN容器，支持cgroups资源隔离、Docker/runc容器运行、
 * 流量控制等功能，是YARN NodeManager在Linux节点上的原生权限控制入口程序。
 * 该程序通常以setuid root方式运行，通过严格的权限校验保证安全性。
 */

#include "config.h"
#include "configuration.h"
#include "container-executor.h"
#include "util.h"
#include "get_executable.h"
#include "modules/gpu/gpu-module.h"
#include "modules/fpga/fpga-module.h"
#include "modules/cgroups/cgroups-operations.h"
#include "modules/devices/devices-module.h"
#include "utils/string-utils.h"
#include "runc/runc.h"
#include "runc/runc_reap.h"

#include <errno.h>
#include <grp.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

/**
 * @brief 打印容器执行器使用帮助信息
 * @param stream 输出流（stdout/stderr）
 */
static void display_usage(FILE *stream) {
  const char* disabled = "[DISABLED]";
  const char* enabled  = "      ";

  fputs("Usage: container-executor --checksetup\n"
        "       container-executor --mount-cgroups <hierarchy> "
        "<controller=path>...\n", stream);

  const char* de = is_tc_support_enabled() ? enabled : disabled;
  fprintf(stream,
      "%s container-executor --tc-modify-state <command-file>\n"
      "%s container-executor --tc-read-state <command-file>\n"
      "%s container-executor --tc-read-stats <command-file>\n",
      de, de, de);

  de = is_terminal_support_enabled() ? enabled : disabled;
  fprintf(stream, "%s container-executor --exec-container <command-file>\n", de);

  de = is_docker_support_enabled() ? enabled : disabled;
  fprintf(stream, "%s container-executor --run-docker <command-file>\n", de);
  fprintf(stream, "%s container-executor --remove-docker-container [hierarchy] <container_id>\n", de);
  fprintf(stream, "%s container-executor --inspect-docker-container <container_id>\n", de);

  de = is_runc_support_enabled() ? enabled : disabled;
  fprintf(stream,
      "%s container-executor --run-runc-container <command-file>\n", de);
  fprintf(stream,
      "%s container-executor --reap-runc-layer-mounts <retain-count>\n", de);

  fprintf(stream,
      "       container-executor <user> <yarn-user> <command> <command-args>\n"
      "       where command and command-args: \n" \
      "            initialize container:  %2d appid containerid tokens nm-local-dirs "
      "nm-log-dirs cmd...\n"
      "            launch container:      %2d appid containerid workdir "
      "container-script tokens http-option pidfile nm-local-dirs nm-log-dirs resources ",
      INITIALIZE_CONTAINER, LAUNCH_CONTAINER);

  if(is_tc_support_enabled()) {
    fputs("optional-tc-command-file\n", stream);
  } else {
    fputs("\n", stream);
  }

  fputs(
      "                                      where http-option is one of:\n"
      "                                      --http\n"
      "                                      --https keystorepath truststorepath\n", stream);

  de = is_docker_support_enabled() ? enabled : disabled;
  fprintf(stream,
      "%11s launch docker container:%2d appid containerid workdir "
      "container-script tokens http-option pidfile nm-local-dirs nm-log-dirs "
      "docker-command-file resources ", de, LAUNCH_DOCKER_CONTAINER);

  if(is_tc_support_enabled()) {
    fputs("optional-tc-command-file\n", stream);
  } else {
    fputs("\n", stream);
  }

  fputs(
      "                                      where http-option is one of:\n"
      "                                      --http\n"
      "                                      --https keystorepath truststorepath\n", stream);

  fprintf(stream,
      "            signal container:      %2d container-pid signal\n"
      "            delete as user:        %2d relative-path\n"
      "            list as user:          %2d relative-path\n",
      SIGNAL_CONTAINER, DELETE_AS_USER, LIST_AS_USER);

  if(is_yarn_sysfs_support_enabled()) {
    fprintf(stream,
        "            sync yarn sysfs:       %2d app-id nm-local-dirs\n",
        SYNC_YARN_SYSFS);
  } else {
    fprintf(stream,
        "[DISABLED]  sync yarn sysfs:       %2d app-id nm-local-dirs\n",
        SYNC_YARN_SYSFS);
  }
  fflush(stream);
}

/* Sets up log files for normal/error logging */
/**
 * @brief 初始化标准输出/错误日志，设置行缓冲并忽略SIGPIPE信号
 */
static void open_log_files() {
  if (LOGFILE == NULL) {
    LOGFILE = stdout;
    if (setvbuf(LOGFILE, NULL, _IOLBF, BUFSIZ)) {
      fprintf(LOGFILE, "Failed to invoke setvbuf() for LOGFILE: %s\n", strerror(errno));
      fflush(LOGFILE);
      exit(ERROR_CALLING_SETVBUF);
    }
  }

  if (ERRORFILE == NULL) {
    ERRORFILE = stderr;
    if (setvbuf(ERRORFILE, NULL, _IOLBF, BUFSIZ)) {
      fprintf(ERRORFILE, "Failed to invoke setvbuf() for ERRORFILE: %s\n", strerror(errno));
      fflush(ERRORFILE);
      exit(ERROR_CALLING_SETVBUF);
    }
  }

  // There may be a process reading from stdout/stderr, and if it
  // exits, we will crash on a SIGPIPE when we try to write to them.
  // By ignoring SIGPIPE, we can handle the EPIPE instead of crashing.
  // 忽略SIGPIPE信号，避免对端退出时本进程被异常终止
  signal(SIGPIPE, SIG_IGN);
}

/* Flushes and closes log files */
/**
 * @brief 刷新关闭日志文件，释放配置相关内存
 */
static void flush_and_close_log_files() {
  if (LOGFILE != NULL) {
    fflush(LOGFILE);
    fclose(LOGFILE);
    LOGFILE = NULL;
  }

  if (ERRORFILE != NULL) {
    fflush(ERRORFILE);
    fclose(ERRORFILE);
    ERRORFILE = NULL;
  }

  free_executor_configurations();
}

/** Validates the current container-executor setup. Causes program exit
in case of validation failures. Also sets up configuration / group information etc.,
This function is to be called in every invocation of container-executor, irrespective
of whether an explicit checksetup operation is requested. */

/**
 * @brief 校验容器执行器运行环境，加载配置，校验权限，设置用户组
 * @param argv0 程序自身路径
 *
 * 每次调用容器执行器都会先执行该校验，确保二进制权限、配置文件权限符合安全要求
 * 校验不通过直接退出，返回对应错误码
 */
static void assert_valid_setup(char *argv0) {
  int ret;
  char *executable_file = get_executable(argv0);
  if (!executable_file || executable_file[0] == 0) {
    fprintf(ERRORFILE, "realpath of executable: %s\n",
            errno != 0 ? strerror(errno) : "unknown");
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }

  char *conf_file = get_config_path(argv0);

  if (conf_file == NULL) {
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }

  if (check_configuration_permissions(conf_file) != 0) {
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }
  read_executor_config(conf_file);
  free(conf_file);

  // look up the node manager group in the config file
  // 从配置中获取NodeManager用户组信息
  char *nm_group = get_nodemanager_group();
  if (nm_group == NULL) {
    free(executable_file);
    fprintf(ERRORFILE, "Can't get configured value for %s.\n", NM_GROUP_KEY);
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }
  struct group *group_info = getgrnam(nm_group);
  if (group_info == NULL) {
    free(executable_file);
    fprintf(ERRORFILE, "Can't get group information for %s - %s.\n", nm_group,
      errno != 0 ? strerror(errno) : "unknown");
    flush_and_close_log_files();
    exit(INVALID_CONFIG_FILE);
  }
  set_nm_uid(getuid(), group_info->gr_gid);
  /*
   * if we are running from a setuid executable, make the real uid root
   * we're going to ignore this result just in case we aren't.
   */
  // 如果是setuid运行，设置真实uid为root，忽略返回值处理非setuid场景
  ret=setuid(0);

  /*
   * set the real and effective group id to the node manager group
   * we're going to ignore this result just in case we aren't
   */
  // 设置真实/有效gid为NodeManager组，忽略返回值
  ret=setgid(group_info->gr_gid);

  /* make the unused var warning to away */
  // 消除未使用变量编译警告
  ret++;

  if (check_executor_permissions(executable_file) != 0) {
    free(executable_file);
    fprintf(ERRORFILE, "Invalid permissions on container-executor binary.\n");
    flush_and_close_log_files();
    exit(INVALID_CONTAINER_EXEC_PERMISSIONS);
  }
  free(executable_file);
}

/**
 * @brief 打印功能未启用错误信息
 * @param name 功能名称
 */
static void display_feature_disabled_message(const char* name) {
    fprintf(ERRORFILE, "Feature disabled: %s\n", name);
}

/* Use to store parsed input parameters for various operations */
/** 存储解析后的命令行输入参数 */
static struct {
  char *cgroups_hierarchy;
  char *traffic_control_command_file;
  const char *run_as_user_name;
  const char *yarn_user_name;
  char *local_dirs;
  char *log_dirs;
  char *resources_key;
  char *resources_value;
  char **resources_values;
  const char *app_id;
  const char *container_id;
  int https;
  const char *keystore_file;
  const char *truststore_file;
  const char *cred_file;
  const char *script_file;
  const char *current_dir;
  const char *pid_file;
  const char *target_dir;
  int container_pid;
  int signal;
  int runc_layer_count;
  const char *command_file;
} cmd_input;

static int validate_run_as_user_commands(int argc, char **argv, int *operation);

/* Validates that arguments used in the invocation are valid. In case of validation
failure, an 'errorcode' is returned. In case of successful validation, a zero is
returned and 'operation' is populated based on the operation being requested.
Ideally, we should re-factor container-executor to use a more structured, command
line parsing mechanism (e.g getopt). For the time being, we'll use this manual
validation mechanism so that we don't have to change the invocation interface.
*/

/**
 * @brief 验证并解析命令行参数，识别请求操作类型
 * @param argc 参数个数
 * @param argv 参数数组
 * @param operation 输出识别出的操作类型
 * @return 0成功，非0对应错误码
 */
static int validate_arguments(int argc, char **argv , int *operation) {
  if (argc < 2) {
    display_usage(stdout);
    return INVALID_ARGUMENT_NUMBER;
  }

  /*
   * Check if it is a known module, if yes, redirect to module
   */
  // 检查是否是GPU模块请求，转发给GPU处理
  if (strcmp("--module-gpu", argv[1]) == 0) {
    return handle_gpu_request(&update_cgroups_parameters, "gpu", argc - 1,
           &argv[1]);
  }

  // 检查是否是FPGA模块请求，转发给FPGA处理
  if (strcmp("--module-fpga", argv[1]) == 0) {
    return handle_fpga_request(&update_cgroups_parameters, "fpga", argc - 1,
           &argv[1]);
  }

  // 检查是否是设备模块请求，转发给设备处理
  if (strcmp("--module-devices", argv[1]) == 0) {
    return handle_devices_request(&update_cgroups_parameters, "devices", argc - 1,
          &argv[1]);
  }

  if (strcmp("--checksetup", argv[1]) == 0) {
    *operation = CHECK_SETUP;
    return 0;
  }

  if (strcmp("--mount-cgroups", argv[1]) == 0) {
    if (is_mount_cgroups_support_enabled()) {
      if (argc < 4) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.cgroups_hierarchy = argv[optind++];
      *operation = MOUNT_CGROUPS;
      return 0;
    } else {
      display_feature_disabled_message("mount cgroup");
      return FEATURE_DISABLED;
    }
  }

  if (strcmp("--tc-modify-state", argv[1]) == 0) {
    if(is_tc_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.traffic_control_command_file = argv[optind++];
      *operation = TRAFFIC_CONTROL_MODIFY_STATE;
      return 0;
    } else {
      display_feature_disabled_message("traffic control");
      return FEATURE_DISABLED;
    }
  }

  if (strcmp("--tc-read-state", argv[1]) == 0) {
    if(is_tc_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.traffic_control_command_file = argv[optind++];
      *operation = TRAFFIC_CONTROL_READ_STATE;
      return 0;
    } else {
      display_feature_disabled_message("traffic control");
      return FEATURE_DISABLED;
    }
  }

  if (strcmp("--tc-read-stats", argv[1]) == 0) {
    if(is_tc_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.traffic_control_command_file = argv[optind++];
      *operation = TRAFFIC_CONTROL_READ_STATS;
      return 0;
    } else {
      display_feature_disabled_message("traffic control");
      return FEATURE_DISABLED;
    }
  }

  if (strcmp("--exec-container", argv[1]) == 0) {
    if(is_terminal_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);
        return INVALID_ARGUMENT_NUMBER;
      }
      optind++;
      cmd_input.command_file = argv[optind++];
      *operation = EXEC_CONTAINER;
      return 0;
    } else {
        display_feature_disabled_message("feature.terminal.enabled");
        return FEATURE_DISABLED;
    }
  }

  if (strcmp("--run-docker", argv[1]) == 0) {
    if(is_docker_support_enabled()) {
      if (argc != 3) {
        display_usage(stdout);