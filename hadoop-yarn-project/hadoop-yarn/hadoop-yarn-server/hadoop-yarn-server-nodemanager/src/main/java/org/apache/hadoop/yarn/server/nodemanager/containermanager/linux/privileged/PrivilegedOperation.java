// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 表示需要更高系统权限才能执行的操作，这些操作通过container-executor二进制程序完成，
 * 例如创建cgroup、以指定用户启动容器、流量控制tc命令操作等。
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrivilegedOperation {
  /** Linux文件路径分隔符，特殊标记用于参数传递 */
  public final static char LINUX_FILE_PATH_SEPARATOR = '%';

  /**
   * 特权操作类型枚举，每种类型对应container-executor的CLI选项
   */
  public enum OperationType {
    /** 检查安装环境 */
    CHECK_SETUP("--checksetup"),
    /** 挂载cgroup文件系统 */
    MOUNT_CGROUPS("--mount-cgroups"),
    /** 初始化容器 */
    INITIALIZE_CONTAINER(""), //no CLI switch supported yet
    /** 启动容器 */
    LAUNCH_CONTAINER(""), //no CLI switch supported yet
    /** 向容器发送信号 */
    SIGNAL_CONTAINER(""), //no CLI switch supported yet
    /** 执行容器命令 */
    EXEC_CONTAINER("--exec-container"), //no CLI switch supported yet
    /** 以指定用户删除文件 */
    DELETE_AS_USER(""), //no CLI switch supported yet
    /** 启动Docker容器 */
    LAUNCH_DOCKER_CONTAINER(""), //no CLI switch supported yet
    /** 修改流量控制状态 */
    TC_MODIFY_STATE("--tc-modify-state"),
    /** 读取流量控制状态 */
    TC_READ_STATE("--tc-read-state"),
    /** 读取流量控制统计 */
    TC_READ_STATS("--tc-read-stats"),
    /** 将进程PID添加到cgroup */
    ADD_PID_TO_CGROUP(""), //no CLI switch supported yet.
    /** 执行Docker命令 */
    RUN_DOCKER_CMD("--run-docker"),
    /** GPU设备操作模块 */
    GPU("--module-gpu"),
    /** FPGA设备操作模块 */
    FPGA("--module-fpga"),
    /** 通用设备操作模块 */
    DEVICE("--module-devices"),
    /** 以指定用户列出文件 */
    LIST_AS_USER(""), // no CLI switch supported yet.
    /** 添加NUMA参数 */
    ADD_NUMA_PARAMS(""), // no CLI switch supported yet.
    /** 删除Docker容器 */
    REMOVE_DOCKER_CONTAINER("--remove-docker-container"),
    /** 检查Docker容器信息 */
    INSPECT_DOCKER_CONTAINER("--inspect-docker-container"),
    /** 同步YARN sysfs信息 */
    SYNC_YARN_SYSFS(""),
    /** 启动runc容器 */
    RUN_RUNC_CONTAINER("--run-runc-container"),
    /** 回收runc层挂载点 */
    REAP_RUNC_LAYER_MOUNTS("--reap-runc-layer-mounts");

    private final String option;

    OperationType(String option) {
      this.option = option;
    }

    /** 获取对应CLI选项字符串 */
    public String getOption() {
      return option;
    }
  }

  /** cgroup参数前缀 */
  public static final String CGROUP_ARG_PREFIX = "cgroups=";
  /** 表示无任务的cgroup参数值 */
  public static final String CGROUP_ARG_NO_TASKS = "none";

  private final OperationType opType;
  private final List<String> args;
  private boolean failureLogging;

  /**
   * 构造指定类型的特权操作
   * @param opType 操作类型
   */
  public PrivilegedOperation(OperationType opType) {
    this.opType = opType;
    this.args = new ArrayList<String>();
    this.failureLogging = true;
  }

  /**
   * 构造指定类型和单个参数的特权操作
   * @param opType 操作类型
   * @param arg 操作参数
   */
  public PrivilegedOperation(OperationType opType, String arg) {
    this(opType);

    if (arg != null) {
      this.args.add(arg);
    }
  }

  /**
   * 构造指定类型和参数列表的特权操作
   * @param opType 操作类型
   * @param args 操作参数列表
   */
  public PrivilegedOperation(OperationType opType, List<String> args) {
    this(opType);

    if (args != null) {
      this.args.addAll(args);
    }
  }

  /**
   * 追加多个参数到操作参数列表
   * @param args 要追加的参数数组
   */
  public void appendArgs(String... args) {
    for (String arg : args) {
      this.args.add(arg);
    }
  }

  /**
   * 追加参数列表到操作参数列表
   * @param args 要追加的参数列表
   */
  public void appendArgs(List<String> args) {
    this.args.addAll(args);
  }

  /** 启用操作失败日志 */
  public void enableFailureLogging() {
    this.failureLogging = true;
  }

  /** 禁用操作失败日志 */
  public void disableFailureLogging() {
    this.failureLogging = false;
  }

  /** 检查是否启用失败日志 */
  public boolean isFailureLoggingEnabled() {
    return failureLogging;
  }

  /** 获取操作类型 */
  public OperationType getOperationType() {
    return opType;
  }

  /** 获取不可修改的参数列表 */
  public List<String> getArguments() {
    return Collections.unmodifiableList(this.args);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PrivilegedOperation)) {
      return false;
    }

    PrivilegedOperation otherOp = (PrivilegedOperation) other;

    return otherOp.opType.equals(opType) && otherOp.args.equals(args);
  }

  @Override
  public int hashCode() {
    return opType.hashCode() + 97 * args.hashCode();
  }

  /**
   * container-executor将执行的以指定用户运行的命令枚举
   */
  public enum RunAsUserCommand {
    /** 初始化容器 */
    INITIALIZE_CONTAINER(0),
    /** 启动容器 */
    LAUNCH_CONTAINER(1),
    /** 向容器发送信号 */
    SIGNAL_CONTAINER(2),
    /** 以指定用户删除 */
    DELETE_AS_USER(3),
    /** 启动Docker容器 */
    LAUNCH_DOCKER_CONTAINER(4),
    /** 以指定用户列出 */
    LIST_AS_USER(5),
    /** 同步YARN sysfs */
    SYNC_YARN_SYSFS(6);

    private int value;
    RunAsUserCommand(int value) {
      this.value = value;
    }
    /** 获取命令枚举对应整数值 */
    public int getValue() {
      return value;
    }
  }

  /**
   * container-executor返回的结果码枚举，必须和container-executor.h中的定义保持一致
   */
  public enum ResultCode {
    /** 执行成功 */
    OK(0),
    /** 无效用户名 */
    INVALID_USER_NAME(2),
    /** 无法执行容器脚本 */
    UNABLE_TO_EXECUTE_CONTAINER_SCRIPT(7),
    /** 无效容器PID */
    INVALID_CONTAINER_PID(9),
    /** 容器执行权限无效 */
    INVALID_CONTAINER_EXEC_PERMISSIONS(22),
    /** 配置文件无效 */
    INVALID_CONFIG_FILE(24),
    /** 写入cgroup失败 */
    WRITE_CGROUP_FAILED(27);

    private final int value;
    ResultCode(int value) {
      this.value = value;
    }
    /** 获取结果码对应整数值 */
    public int getValue() {
      return value;
    }
  }
}