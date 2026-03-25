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
 * @file configuration.c
 * @brief YARN NodeManager容器执行器配置解析模块，负责配置文件读取、权限检查和键值对解析
 */

// ensure we get the posix version of dirname by including this first
#include <libgen.h>

#include "configuration.h"
#include "util.h"
#include "get_executable.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define MAX_SIZE 10

static const char COMMENT_BEGIN_CHAR = '#';
static const char SECTION_LINE_BEGIN_CHAR = '[';
static const char SECTION_LINE_END_CHAR = ']';

/**
 * @brief 释放配置section的内存
 * @param section 需要释放的section对象
 */
//clean up method for freeing section
void free_section(struct section *section) {
  int i = 0;
  for (i = 0; i < section->size; i++) {
    if (section->kv_pairs[i]->key != NULL) {
      free((void *) section->kv_pairs[i]->key);
    }
    if (section->kv_pairs[i]->value != NULL) {
      free((void *) section->kv_pairs[i]->value);
    }
    free(section->kv_pairs[i]);
  }
  if (section->kv_pairs) {
    free(section->kv_pairs);
    section->kv_pairs = NULL;
  }
  if (section->name) {
    free(section->name);
    section->name = NULL;
  }
  section->size = 0;
  free(section);
}

/**
 * @brief 释放整个配置对象的内存
 * @param cfg 需要释放的配置对象
 */
//clean up method for freeing configuration
void free_configuration(struct configuration *cfg) {
  int i = 0;
  for (i = 0; i < cfg->size; i++) {
    if (cfg->sections[i] != NULL) {
      free_section(cfg->sections[i]);
    }
  }
  if (cfg->sections) {
    free(cfg->sections);
  }
  cfg->size = 0;
}

/**
 * @brief 检查文件或目录是否仅root可写，安全检查防止攻击者篡改配置
 * @param file 要检查的文件路径
 * @return 1表示仅root可写，0表示权限不符合要求
 */
/**
 * Is the file/directory only writable by root.
 * Returns 1 if true
 */
static int is_only_root_writable(const char *file) {
  struct stat file_stat;
  if (stat(file, &file_stat) != 0) {
    fprintf(ERRORFILE, "Can't stat file %s - %s\n", file, strerror(errno));
    return 0;
  }
  if (file_stat.st_uid != 0) {
    fprintf(ERRORFILE, "File %s must be owned by root, but is owned by %" PRId64 "\n",
            file, (int64_t) file_stat.st_uid);
    return 0;
  }
  if ((file_stat.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
    fprintf(ERRORFILE,
            "File %s must not be world or group writable, but is %03lo\n",
            file, (unsigned long) file_stat.st_mode & (~S_IFMT));
    return 0;
  }
  return 1;
}

/**
 * @brief 解析配置文件路径，将相对路径转换为绝对路径
 * @param file_name 配置文件名
 * @param root 相对路径根目录，相对路径会相对于此目录解析
 * @return 解析后的绝对路径，需要调用者释放内存
 *
 * NOTE: relative path names are resolved relative to the second argument not getwd(3)
 */
/**
 * Return a string with the configuration file path name resolved via realpath(3)
 *
 * NOTE: relative path names are resolved relative to the second argument not getwd(3)
 */
char *resolve_config_path(const char *file_name, const char *root) {
  const char *real_fname = NULL;
  char buffer[EXECUTOR_PATH_MAX * 2 + 1];

  // 已经是绝对路径，直接使用
  if (file_name[0] == '/') {
    real_fname = file_name;
  } else if (realpath(root, buffer) != NULL) {
    // 相对路径，拼接根目录得到完整路径
    strncpy(strrchr(buffer, '/') + 1, file_name, EXECUTOR_PATH_MAX);
    real_fname = buffer;
  }

#ifdef HAVE_CANONICALIZE_FILE_NAME
  char * ret = (real_fname == NULL) ? NULL : canonicalize_file_name(real_fname);
#else
  char *ret = (real_fname == NULL) ? NULL : realpath(real_fname, NULL);
#endif
#ifdef DEBUG
  fprintf(stderr,"ret = %s\n", ret);
  fprintf(stderr, "resolve_config_path(file_name=%s,root=%s)=%s\n",
          file_name, root ? root : "null", ret ? ret : "null");
#endif
  return ret;
}

/**
 * @brief 检查配置文件及其所有父目录的权限，确保只有root可写，防止配置被篡改
 * @param file_name 配置文件路径
 * @return 0表示权限检查通过，-1表示检查失败
 * returns 0 if permissions are ok
 */
/**
 * Ensure that the configuration file and all of the containing directories
 * are only writable by root. Otherwise, an attacker can change the
 * configuration and potentially cause damage.
 * returns 0 if permissions are ok
 */
int check_configuration_permissions(const char *file_name) {
  if (!file_name) {
    return -1;
  }

  // 复制路径字符串供dirname修改，dirname会修改输入字符串
  char *dir = strdup(file_name);
  if (!dir) {
    fprintf(stderr, "Failed to make a copy of filename in %s.\n", __func__);
    return -1;
  }

  char *buffer = dir;
  do {
    if (!is_only_root_writable(dir)) {
      free(buffer);
      return -1;
    }
    // 向上移动到父目录继续检查
    dir = dirname(dir);
  } while (strcmp(dir, "/") != 0);
  free(buffer);
  return 0;
}

/**
 * @brief 从配置文件读取一行，去除末尾换行符
 * @param conf_file 配置文件指针
 * @return 读取到的行，需要调用者释放内存；文件结束返回NULL
 * The caller must free the memory allocated.
 */
/**
 * Read a line from the the config file and return it without the newline.
 * The caller must free the memory allocated.
 */
static char *read_config_line(FILE *conf_file) {
  char *line = NULL;
  size_t linesize = 100000;
  ssize_t size_read = 0;
  size_t eol = 0;

  line = (char *) malloc(linesize);
  if (line == NULL) {
    fprintf(ERRORFILE, "malloc failed while reading configuration file.\n");
    exit(OUT_OF_MEMORY);
  }
  size_read = getline(&line, &linesize, conf_file);

  //feof returns true only after we read past EOF.
  //so a file with no new line, at last can reach this place
  //if size_read returns negative check for eof condition
  if (size_read == -1) {
    free(line);
    line = NULL;
    if (!feof(conf_file)) {
      fprintf(ERRORFILE, "Line read returned -1 without eof\n");
      exit(INVALID_CONFIG_FILE);
    }
  } else {
    eol = strlen(line) - 1;
    if (line[eol] == '\n') {
      // 去除末尾换行符
      line[eol] = '\0';
    }
  }
  return line;
}

/**
 * @brief 判断给定行是否是注释行
 * @param line 要检查的行
 * @return 1表示是注释行，0表示不是
 */
/**
 * Return if the given line is a comment line.
 *
 * @param line the line to check
 *
 * @return 1 if the line is a comment line, 0 otherwise
 */
static int is_comment_line(const char *line) {
  if (line != NULL) {
    return (line[0] == COMMENT_BEGIN_CHAR);
  }
  return 0;
}

/**
 * @brief 判断给定行是否是section起始行
 * @param line 要检查的行
 * @return 1表示是section起始行，0表示不是
 */
/**
 * Return if the given line is a section start line.
 *
 * @param line the line to check
 *
 * @return 1 if the line is a section start line, 0 otherwise
 */
static int is_section_start_line(const char *line) {
  size_t len = 0;
  if (line != NULL) {
    len = strlen(line) - 1;
    return (line[0] == SECTION_LINE_BEGIN_CHAR
            && line[len] == SECTION_LINE_END_CHAR);
  }
  return 0;
}

/**
 * @brief 从section起始行提取section名称
 * @param line section起始行，格式如[section-name]
 * @return 提取出的section名称，需要调用者释放内存；失败返回NULL
 */
/**
 * Return the name of the section from the given section start line. The
 * caller must free the memory used.
 *
 * @param line the line to extract the section name from
 *
 * @return string with the name of the section, NULL otherwise
 */
static char *get_section_name(const char *line) {
  char *name = NULL;
  size_t len;

  if (is_section_start_line(line)) {
    // 长度减去前后括号各占一个字符
    len = strlen(line) - 2;
    name = (char *) malloc(len + 1);
    if (name == NULL) {
      fprintf(ERRORFILE, "malloc failed while reading section name.\n");
      exit(OUT_OF_MEMORY);
    }
    strncpy(name, line + sizeof(char), len);
    name[len] = '\0';
  }
  return name;
}

/**
 * @brief 从行中读取键值对条目，存入section
 * @param line 要解析的行
 * @param section 存储解析结果的section对象
 * @return 0表示解析成功；<0表示配置错误；>0表示空行等非错误问题
 *
 * @return 0 if an entry was found
 *         <0 for config file errors
 *         >0 for issues such as empty line
 *
 */
/**
 * Read an entry for the section from the line. Function returns 0 if an entry
 * was found, non-zero otherwise. Return values less than 0 indicate an error
 * with the config file.
 *
 * @param line the line to read the entry from
 * @param section the struct to read the entry into
 *
 * @return 0 if an entry was found
 *         <0 for config file errors
 *         >0 for issues such as empty line
 *
 */
static int read_section_entry(const char *line, struct section *section) {
  char *equaltok;
  char *temp_equaltok;
  const char *splitter = "=";
  char *buffer;
  size_t len = 0;
  if (line == NULL || section == NULL) {
    fprintf(ERRORFILE, "NULL params passed to read_section_entry");
    return -1;
  }
  len = strlen(line);
  if (len == 0) {
    return 1;
  }
  // 每达到MAX_SIZE扩容一次
  if ((section->size) % MAX_SIZE == 0) {
    section->kv_pairs = (struct kv_pair **) realloc(
        section->kv_pairs,
        sizeof(struct kv_pair *) * (MAX_SIZE + section->size));
    if (section->kv_pairs == NULL) {
      fprintf(ERRORFILE,
              "Failed re-allocating memory for configuration items\n");
      exit(OUT_OF_MEMORY);
    }
  }

  buffer = strdup(line);
  if (!buffer) {
    fprintf(ERRORFILE, "Failed to allocating memory for line, %s\n", __func__);
    exit(OUT_OF_MEMORY);
  }

  // 按等号分割键和值
  //if no equals is found ignore this line, can be an empty line also
  equaltok = strtok_r(buffer, splitter, &temp_equaltok);
  if (equaltok == NULL) {
    fprintf(ERRORFILE, "Error with line '%s', no '=' found\n", buffer);
    exit(INVALID_CONFIG_FILE);
  }
  section->kv_pairs[section->size] = (struct kv_pair *) malloc(
      sizeof(struct kv_pair));
  if (section->kv_pairs[section->size] == NULL) {
    fprintf(ERRORFILE, "Failed allocating memory for single section item\n");
    exit(OUT_OF_MEMORY);
  }
  memset(section->kv_pairs[section->size], 0,
         sizeof(struct kv_pair));
  // 修剪键前后空白字符
  section->kv_pairs[section->size]->key = trim(equaltok);

  equaltok = strtok_r(NULL, splitter, &temp_equaltok);
  if (equaltok == NULL) {
    int has_values = 1;
    if (strstr(line, splitter) == NULL) {
      fprintf(ERRORFILE, "configuration tokenization failed, error with line %s\n", line);
      has_values = 0;
    }

    // 无效行，释放已分配内存
    free((void *) section->kv_pairs[section->size]->key);
    free((void *) section->kv_pairs[section->size]);
    section->kv_pairs[section->size] = NULL;
    free(buffer);

    if (!has_values) {
      return -1;
    }

    return 2;
  }

#ifdef DEBUG
  fprintf(LOGFILE, "read_config : Adding conf value : %s \n", equaltok);
#endif

  // 修剪值前后空白字符
  section->kv_pairs[section->size]->value = trim(equaltok);
  section->size++;
  free(buffer);
  return 0;
}

/**
 * @brief 移除行末尾的注释，直接修改传入的行字符串
 * @param line 要处理的行字符串
 */
/**
 * Remove any trailing comment from the supplied line. Function modifies the
 * argument provided.
 *
 * @param line the line from which to remove the comment
 */
static void trim_comment(char *line) {
  char *begin_comment = NULL;
  if (line != NULL) {
    begin_comment = strchr(line, COMMENT_BEGIN_CHAR);
    if (begin_comment != NULL) {
      *begin_comment = '\0';
    }
  }
}

/**
 * @brief 分配并初始化一个空的section对象
 * @return 初始化完成的section对象指针，分配失败直接退出进程
 *
 */
/**
 * Allocate a section struct and initialize it. The memory must be freed by
 * the caller. Function calls exit if any error occurs.
 *
 * @return pointer to the allocated section struct
 *
 */
static struct section *allocate_section() {
  struct section *section = (struct section *) malloc(sizeof(struct section));
  if (section == NULL) {
    fprintf(ERRORFILE, "malloc failed while allocating section.\n");
    exit(OUT_OF_MEMORY);
  }
  section->name = NULL;
  section->kv_pairs = NULL;
  section->size = 0;
  return section;
}

/**
 * @brief 从配置文件读取当前section的所有键值对
 * @param conf_file 配置文件指针
 * @param section 存储读取结果的section对象
 *
 */
/**
 * Populate the given section struct with fields from the config file.
 *
 * @param conf_file the file to read from
 * @param section pointer to the section struct to populate
 *
 */
static void populate_section_fields(FILE *conf_file, struct section *section) {
  char *line;
  long int offset = 0;
  while (!feof(conf_file)) {
    offset = ftell