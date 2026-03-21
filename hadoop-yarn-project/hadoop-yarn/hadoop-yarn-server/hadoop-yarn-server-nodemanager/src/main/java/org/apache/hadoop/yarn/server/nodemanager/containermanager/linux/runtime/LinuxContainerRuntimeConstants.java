// 这个文件已经全部加上中文注释
/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext.Attribute;

import java.util.List;
import java.util.Map;

/**
 * Linux容器运行时常量定义类，存储所有Linux容器运行时通用的常量属性定义。
 */
public final class LinuxContainerRuntimeConstants {
  private LinuxContainerRuntimeConstants() {
  }

  /**
   * Linux容器运行时类型枚举，定义DelegatingLinuxContainerRuntime支持的各类运行时。
   */
  public enum RuntimeType {
    DEFAULT,
    DOCKER,
    JAVASANDBOX,
    RUNC;
  }

  // 本地化资源信息属性
  public static final Attribute<Map> LOCALIZED_RESOURCES = Attribute
      .attribute(Map.class, "localized_resources");
  // 容器启动前缀命令列表属性
  public static final Attribute<List> CONTAINER_LAUNCH_PREFIX_COMMANDS =
      Attribute.attribute(List.class, "container_launch_prefix_commands");
  // 容器运行用户属性
  public static final Attribute<String> RUN_AS_USER =
      Attribute.attribute(String.class, "run_as_user");
  // 容器对应用户属性
  public static final Attribute<String> USER = Attribute.attribute(String.class,
      "user");
  // 应用ID属性
  public static final Attribute<String> APPID =
      Attribute.attribute(String.class, "appid");
  // 容器ID字符串属性
  public static final Attribute<String> CONTAINER_ID_STR = Attribute
      .attribute(String.class, "container_id_str");
  // 容器工作目录路径属性
  public static final Attribute<Path> CONTAINER_WORK_DIR = Attribute
      .attribute(Path.class, "container_work_dir");
  // NodeManager私有容器脚本路径属性
  public static final Attribute<Path> NM_PRIVATE_CONTAINER_SCRIPT_PATH =
      Attribute.attribute(Path.class, "nm_private_container_script_path");
  // NodeManager私有令牌文件路径属性
  public static final Attribute<Path> NM_PRIVATE_TOKENS_PATH = Attribute
      .attribute(Path.class, "nm_private_tokens_path");
  // NodeManager私有密钥库路径属性
  public static final Attribute<Path> NM_PRIVATE_KEYSTORE_PATH = Attribute
      .attribute(Path.class, "nm_private_keystore_path");
  // NodeManager私有信任库路径属性
  public static final Attribute<Path> NM_PRIVATE_TRUSTSTORE_PATH = Attribute
      .attribute(Path.class, "nm_private_truststore_path");
  // PID文件路径属性
  public static final Attribute<Path> PID_FILE_PATH = Attribute.attribute(
      Path.class, "pid_file_path");
  // 本地目录列表属性
  public static final Attribute<List> LOCAL_DIRS = Attribute.attribute(
      List.class, "local_dirs");
  // 日志目录列表属性
  public static final Attribute<List> LOG_DIRS = Attribute.attribute(
      List.class, "log_dirs");
  // 文件缓存目录列表属性
  public static final Attribute<List> FILECACHE_DIRS = Attribute.attribute(
      List.class, "filecache_dirs");
  // 用户本地目录列表属性
  public static final Attribute<List> USER_LOCAL_DIRS = Attribute.attribute(
      List.class, "user_local_dirs");
  // 容器本地目录列表属性
  public static final Attribute<List> CONTAINER_LOCAL_DIRS = Attribute
      .attribute(List.class, "container_local_dirs");
  // 用户文件缓存目录列表属性
  public static final Attribute<List> USER_FILECACHE_DIRS = Attribute
      .attribute(List.class, "user_filecache_dirs");
  // 应用本地目录列表属性
  public static final Attribute<List> APPLICATION_LOCAL_DIRS = Attribute
      .attribute(List.class, "application_local_dirs");
  // 容器日志目录列表属性
  public static final Attribute<List> CONTAINER_LOG_DIRS = Attribute.attribute(
      List.class, "container_log_dirs");
  // 资源选项属性
  public static final Attribute<String> RESOURCES_OPTIONS = Attribute.attribute(
      String.class, "resources_options");
  // tc命令文件路径属性
  public static final Attribute<String> TC_COMMAND_FILE = Attribute.attribute(
      String.class, "tc_command_file");
  // 容器运行命令列表属性
  public static final Attribute<List> CONTAINER_RUN_CMDS = Attribute.attribute(
      List.class, "container_run_cmds");
  // cgroup相对路径属性
  public static final Attribute<String> CGROUP_RELATIVE_PATH = Attribute
      .attribute(String.class, "cgroup_relative_path");

  // 进程ID属性
  public static final Attribute<String> PID = Attribute.attribute(
      String.class, "pid");
  // 终止信号属性
  public static final Attribute<ContainerExecutor.Signal> SIGNAL = Attribute
      .attribute(ContainerExecutor.Signal.class, "signal");
  // proc文件系统路径属性
  public static final Attribute<String> PROCFS = Attribute.attribute(
      String.class, "procfs");
}