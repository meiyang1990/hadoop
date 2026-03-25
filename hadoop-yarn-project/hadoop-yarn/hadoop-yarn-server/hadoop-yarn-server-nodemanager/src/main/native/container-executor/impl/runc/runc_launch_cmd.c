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
 * @file runc_launch_cmd.c
 * @brief runC容器启动命令JSON解析与验证实现
 * @details 负责解析NodeManager下发的runC容器启动命令JSON文件，
 *          验证命令合法性，为后续启动容器提供结构化的配置数据
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "util.h"
#include "utils/cJSON/cJSON.h"
#include "utils/file-utils.h"
#include "utils/string-utils.h"
#include "utils/mount-utils.h"

#include "configuration.h"
#include "container-executor.h"
#include "runc_config.h"
#include "runc_launch_cmd.h"

#define SQUASHFS_MEDIA_TYPE     "application/vnd.squashfs"

/**
 * 释放镜像层描述符数组内存
 */
static void free_rlc_layers(rlc_layer_spec* layers, unsigned int num_layers) {
  for (unsigned int i = 0; i < num_layers; ++i) {
    free(layers[i].media_type);
    free(layers[i].path);
  }
  free(layers);
}

/**
 * 释放以NULL结尾的指针数组内存
 */
static void free_ntarray(char** parray) {
  if (parray != NULL) {
    for (char** p = parray; *p != NULL; ++p) {
      free(*p);
    }
    free(parray);
  }
}

/**
 * 释放runC启动命令结构及其所有关联内存
 */
void free_runc_launch_cmd(runc_launch_cmd* rlc) {
  if (rlc != NULL) {
    free(rlc->run_as_user);
    free(rlc->username);
    free(rlc->app_id);
    free(rlc->container_id);
    free(rlc->pid_file);
    free(rlc->script_path);
    free(rlc->cred_path);
    free_ntarray(rlc->local_dirs);
    free_ntarray(rlc->log_dirs);
    free_rlc_layers(rlc->layers, rlc->num_layers);
    cJSON_Delete(rlc->config.hostname);
    cJSON_Delete(rlc->config.linux_config);
    cJSON_Delete(rlc->config.mounts);
    cJSON_Delete(rlc->config.process.args);
    cJSON_Delete(rlc->config.process.cwd);
    cJSON_Delete(rlc->config.process.env);
    free(rlc);
  }
}

/**
 * 解析JSON文件为cJSON对象，以NodeManager用户权限读取
 * @param filename JSON文件路径
 * @return 解析成功返回cJSON指针，失败返回NULL
 */
static cJSON* parse_json_file(const char* filename) {
  char* data = read_file_to_string_as_nm_user(filename);
  if (data == NULL) {
    fprintf(ERRORFILE, "Cannot read command file %s\n", filename);
    return NULL;
  }

  const char* parse_error_location = NULL;
  cJSON* json = cJSON_ParseWithOpts(data, &parse_error_location, 1);
  if (json == NULL) {
    fprintf(ERRORFILE, "Error parsing command file %s at byte offset %ld\n",
        filename, parse_error_location - data);
  }

  free(data);
  return json;
}

/**
 * 解析JSON数组为目录路径数组（以NULL结尾）
 * @param dirs_json JSON数组对象
 * @return 解析成功返回目录指针数组，失败返回NULL
 */
static char** parse_dir_list(const cJSON* dirs_json) {
  if (!cJSON_IsArray(dirs_json)) {
    return NULL;
  }

  int num_dirs = cJSON_GetArraySize(dirs_json);
  if (num_dirs <= 0) {
    return NULL;
  }

  // 分配额外一个位置存放终止NULL
  char** dirs = calloc(num_dirs + 1, sizeof(*dirs));
  int i = 0;
  const cJSON* e;
  cJSON_ArrayForEach(e, dirs_json) {
    if (!cJSON_IsString(e)) {
      free_ntarray(dirs);
      return NULL;
    }
    dirs[i++] = strdup(e->valuestring);
  }

  return dirs;
}

/**
 * 解析单个镜像层JSON对象到输出结构
 * @param layer_out 输出镜像层结构指针
 * @param layer_json 输入JSON对象
 * @return 解析成功返回true，失败返回false
 */
static bool parse_runc_launch_cmd_layer(rlc_layer_spec* layer_out,
    const cJSON* layer_json) {
  if (!cJSON_IsObject(layer_json)) {
    fputs("runC launch command layer is not an object\n", ERRORFILE);
    return false;
  }

  const cJSON* media_type_json = cJSON_GetObjectItemCaseSensitive(layer_json,
      "mediaType");
  if (!cJSON_IsString(media_type_json)) {
    fputs("Bad/Missing media type for runC launch command layer\n", ERRORFILE);
    return false;
  }

  const cJSON* path_json = cJSON_GetObjectItemCaseSensitive(layer_json, "path");
  if (!cJSON_IsString(path_json)) {
    fputs("Bad/Missing path for runC launch command layer\n", ERRORFILE);
    return false;
  }

  layer_out->media_type = strdup(media_type_json->valuestring);
  layer_out->path = strdup(path_json->valuestring);
  return true;
}

/**
 * 解析JSON数组为镜像层描述符数组
 * @param num_layers_out 输出解析得到的镜像层数量
 * @param layers_json 输入JSON数组
 * @return 解析成功返回镜像层数组指针，失败返回NULL
 */
static rlc_layer_spec* parse_runc_launch_cmd_layers(unsigned int* num_layers_out,
    const cJSON* layers_json) {
  if (!cJSON_IsArray(layers_json)) {
    fputs("Bad/Missing runC launch command layers\n", ERRORFILE);
    return NULL;
  }

  unsigned int num_layers = (unsigned int) cJSON_GetArraySize(layers_json);
  if (num_layers <= 0) {
    return NULL;
  }

  rlc_layer_spec* layers = calloc(num_layers, sizeof(*layers));
  if (layers == NULL) {
    fprintf(ERRORFILE, "Cannot allocate memory for %d layers\n",
        num_layers + 1);
    return NULL;
  }

  unsigned int layer_index = 0;
  const cJSON* e;
  cJSON_ArrayForEach(e, layers_json) {
    if (layer_index >= num_layers) {
      fputs("Iterating past end of layer array\n", ERRORFILE);
      free_rlc_layers(layers, layer_index);
      return NULL;
    }

    if (!parse_runc_launch_cmd_layer(&layers[layer_index], e)) {
      free_rlc_layers(layers, layer_index);
      return NULL;
    }

    ++layer_index;
  }

  *num_layers_out = layer_index;
  return layers;
}

/**
 * 解析JSON节点为整数
 * @param json JSON节点
 * @return 解析成功返回整数值，失败返回-1
 */
static int parse_json_int(cJSON* json) {
  if (!cJSON_IsNumber(json)) {
    fputs("Bad/Missing runC int\n", ERRORFILE);
    return -1;
  }
  return json->valueint;
}

/**
 * 从JSON中分离并解析runC运行配置
 * @param rc 输出runC配置结构指针
 * @param rc_json 输入JSON对象
 * @return 解析成功返回0，失败返回-1
 */
static int parse_runc_launch_cmd_runc_config(runc_config* rc, cJSON* rc_json) {
  if (!cJSON_IsObject(rc_json)) {
    fputs("Bad/Missing runC runtime config in launch command\n", ERRORFILE);
    return -1;
  }
  // 从JSON中分离对应节点，避免重复释放
  rc->hostname = cJSON_DetachItemFromObjectCaseSensitive(rc_json, "hostname");
  rc->linux_config = cJSON_DetachItemFromObjectCaseSensitive(rc_json, "linux");
  rc->mounts = cJSON_DetachItemFromObjectCaseSensitive(rc_json, "mounts");

  cJSON* process_json = cJSON_GetObjectItemCaseSensitive(rc_json, "process");
  if (!cJSON_IsObject(process_json)) {
    fputs("Bad/Missing process section in runC config\n", ERRORFILE);
    return -1;
  }
  rc->process.args = cJSON_DetachItemFromObjectCaseSensitive(
      process_json, "args");
  rc->process.cwd = cJSON_DetachItemFromObjectCaseSensitive(
      process_json, "cwd");
  rc->process.env = cJSON_DetachItemFromObjectCaseSensitive(
      process_json, "env");

  return 0;
}

/**
 * 验证镜像层媒体类型是否支持
 * @param media_type 媒体类型字符串
 * @return 合法返回true，否则返回false
 */
static bool is_valid_layer_media_type(char* media_type) {
  if (media_type == NULL) {
    return false;
  }

  if (strcmp(SQUASHFS_MEDIA_TYPE, media_type)) {
    fprintf(ERRORFILE, "Unrecognized layer media type: %s\n", media_type);
    return false;
  }

  return true;
}

/**
 * 验证所有镜像层是否合法
 * @param layers 镜像层数组
 * @param num_layers 镜像层数量
 * @return 全部合法返回true，否则返回false
 */
static bool is_valid_runc_launch_cmd_layers(rlc_layer_spec* layers,
    unsigned int num_layers) {
  if (layers == NULL) {
    return false;
  }

  for (unsigned int i = 0; i < num_layers; ++i) {
    if (!is_valid_layer_media_type(layers[i].media_type)) {
      return false;
    }
    if (layers[i].path == NULL) {
      return false;
    }
  }

  return true;
}

/**
 * 验证runC Linux资源配置是否合法（只允许已知配置项）
 * @param rclr 资源配置JSON对象
 * @return 合法返回true，否则返回false
 */
static bool is_valid_runc_config_linux_resources(const cJSON* rclr) {
  if (!cJSON_IsObject(rclr)) {
    fputs("runC config linux resources missing or not an object\n", ERRORFILE);
    return false;
  }

  bool all_sections_ok = true;
  const cJSON* e;
  cJSON_ArrayForEach(e, rclr) {
    if (strcmp("blockIO", e->string) == 0) {
      // 允许块IO配置
    } else if (strcmp("cpu", e->string) == 0) {
      // 允许CPU配置
    } else {
      fprintf(ERRORFILE,
          "Unrecognized runC config linux resources element: %s\n", e->string);
      all_sections_ok = false;
    }
  }

  return all_sections_ok;
}

/**
 * 验证runC seccomp安全配置是否合法（只允许已知配置项）
 * @param rcls seccomp配置JSON对象
 * @return 合法返回true，否则返回false
 */
static bool is_valid_runc_config_linux_seccomp(const cJSON* rcls) {
  if (!cJSON_IsObject(rcls)) {
    fputs("runC config linux seccomp missing or not an object\n", ERRORFILE);
    return false;
  }

  bool all_sections_ok = true;
  const cJSON* e;
  cJSON_ArrayForEach(e, rcls) {
    if (strcmp("defaultAction", e->string) == 0) {
      // 允许defaultAction配置
    } else if (strcmp("architectures", e->string) == 0) {
      // 允许架构配置
    } else if (strcmp("flags", e->string) == 0) {
      // 允许flags配置
    } else if (strcmp("syscalls", e->string) == 0) {
      // 允许系统调用配置
    } else {
      fprintf(ERRORFILE,
          "Unrecognized runC config linux seccomp element: %s\n", e->string);
      all_sections_ok = false;
    }
  }

  return all_sections_ok;

}

/**
 * 验证runC Linux整体配置是否合法（只允许已知配置项）
 * @param rcl Linux配置JSON对象
 * @return 合法返回true，否则返回false
 */
static bool is_valid_runc_config_linux(const cJSON* rcl) {
  if (!cJSON_IsObject(rcl)) {
    fputs("runC config linux section missing or not an object\n", ERRORFILE);
    return false;
  }

  bool all_sections_ok = true;
  const cJSON* e;
  cJSON_ArrayForEach(e, rcl) {
    if (strcmp("cgroupsPath", e->string) == 0) {
      if (!cJSON_IsString(e)) {
        all_sections_ok = false;
      }
    } else if (strcmp("resources", e->string) == 0) {
      all_sections_ok &= is_valid_runc_config_linux_resources(e);
    } else if (strcmp("seccomp", e->string) == 0) {
      all_sections_ok &= is_valid_runc_config_linux_seccomp(e);
    } else {
      fprintf(ERRORFILE, "Unrecognized runC config linux element: %s\n",
          e->string);
      all_sections_ok = false;
    }
  }

  return all_sections_ok;
}

/**
 * 验证挂载类型是否合法（只允许bind挂载）
 * @param type 挂载类型字符串
 * @return 合法返回true，否则返回false
 */
static bool is_valid_mount_type(const char *type) {
  if (strcmp("bind", type)) {
    fprintf(ERRORFILE, "Invalid runC mount type '%s'\n", type);
    return false;
  }
  return true;
}

/**
 * 解析挂载选项数组，验证必填选项是否存在
 * @param mo 挂载选项JSON数组
 * @return 解析成功返回挂载选项结构，失败返回NULL
 */
static mount_options* get_mount_options(const cJSON* mo) {
  if (!cJSON_IsArray(mo)) {
    fputs("runC config mount options not an array\n", ERRORFILE);
    return NULL;
  }

  unsigned int num_options = cJSON_GetArraySize(mo);

  mount_options *options = (mount_options *) calloc(1, sizeof(*options));
  char **options_array = (char **) calloc(num_options + 1, sizeof(char*));

  options->num_opts = num_options;
  options->opts = options_array;

  // 标记必填选项是否存在
  bool has_rbind = false;
  bool has_rprivate = false;
  int i = 0;
  const cJSON* e;
  cJSON_ArrayForEach(e, mo) {
    if (!cJSON_IsString(e)) {
      fputs("runC config mount option is not a string\n", ERRORFILE);
      free_mount_options(options);
      return NULL;
    }
    if (strcmp("rbind", e->valuestring) == 0) {
      has_rbind = true;
    } else if (strcmp("rprivate", e->valuestring) == 0) {
      has_rprivate = true;
    } else if (strcmp("rw", e->valuestring) == 0) {
      options->rw = 1;
    } else if (strcmp("ro", e->valuestring) == 0) {
      options->rw = 0;
    }

    options->opts[i] = strdup(e->valuestring);
    i++;
  }
  options->opts[i] = NULL;

  // 检查必填选项
  if (!has_rbind) {
    fputs("runC config mount options missing rbind\n