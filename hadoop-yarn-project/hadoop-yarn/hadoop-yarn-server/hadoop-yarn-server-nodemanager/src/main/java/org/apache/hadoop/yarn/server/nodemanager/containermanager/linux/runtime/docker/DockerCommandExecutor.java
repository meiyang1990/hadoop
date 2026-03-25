// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Docker命令执行工具类，提供Docker容器相关操作的通用执行能力
 * 用于YARN NodeManager管理Docker容器生命周期
 */
public final class DockerCommandExecutor {
  private static final Logger LOG =
       LoggerFactory.getLogger(DockerCommandExecutor.class);

  /**
   * Docker容器状态枚举，定义了所有可能的Docker容器运行状态
   */
  public enum DockerContainerStatus {
    CREATED("created"),
    RUNNING("running"),
    STOPPED("stopped"),
    RESTARTING("restarting"),
    REMOVING("removing"),
    DEAD("dead"),
    EXITED("exited"),
    NONEXISTENT("nonexistent"),
    UNKNOWN("unknown");

    private final String name;

    DockerContainerStatus(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private DockerCommandExecutor() {
  }

  /**
   * 执行指定Docker命令并返回命令执行输出结果
   * 通过特权操作执行器执行，适配YARN的权限模型
   *
   * @param dockerCommand               要执行的Docker命令对象
   * @param containerId                 YARN容器ID
   * @param env                         命令执行环境变量
   * @param privilegedOperationExecutor 特权操作执行器，用于提权执行docker命令
   * @param disableFailureLogging       是否禁用已知错误码的失败日志，避免冗余日志
   * @param nmContext                  NodeManager上下文对象
   * @return Docker命令执行输出结果，已去除首尾空白字符
   * @throws ContainerExecutionException 如果命令执行失败，抛出容器执行异常
   */
  public static String executeDockerCommand(DockerCommand dockerCommand,
      String containerId, Map<String, String> env,
      PrivilegedOperationExecutor privilegedOperationExecutor,
      boolean disableFailureLogging, Context nmContext)
      throws ContainerExecutionException {
    // 准备特权操作，将Docker命令转换为可执行的特权操作对象
    PrivilegedOperation dockerOp = dockerCommand.preparePrivilegedOperation(
        dockerCommand, containerId, env, nmContext);

    // 如果配置禁用失败日志，设置操作禁用失败日志输出
    if (disableFailureLogging) {
      dockerOp.disableFailureLogging();
    }
    LOG.debug("Running docker command: {}", dockerCommand);

    try {
      // 通过特权执行器执行Docker命令，获取执行结果
      String result = privilegedOperationExecutor
          .executePrivilegedOperation(null, dockerOp, null,
              env, true, false);
      // 结果非空时去除首尾空白字符
      if (result != null && !result.isEmpty()) {
        result = result.trim();
      }
      return result;
    } catch (PrivilegedOperationException e) {
      // 捕获特权操作异常，转换为容器执行异常抛出
      throw new ContainerExecutionException("Docker operation failed",
          e.getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  /**
   * 获取指定Docker容器的当前运行状态
   * 通过执行docker inspect命令获取状态，容器不存在时返回NONEXISTENT
   *
   * @param containerId                 要查询状态的容器ID
   * @param privilegedOperationExecutor 特权操作执行器
   * @param nmContext                  NodeManager上下文对象
   * @return 容器当前状态枚举值
   */
  public static DockerContainerStatus getContainerStatus(String containerId,
      PrivilegedOperationExecutor privilegedOperationExecutor,
      Context nmContext) {
    try {
      // 执行状态查询命令，获取状态字符串
      String currentContainerStatus =
          executeStatusCommand(containerId,
          privilegedOperationExecutor, nmContext);
      // 解析状态字符串为枚举值
      DockerContainerStatus dockerContainerStatus = parseContainerStatus(
          currentContainerStatus);
      LOG.debug("Container Status: {} ContainerId: {}",
          dockerContainerStatus.getName(), containerId);

      return dockerContainerStatus;
    } catch (ContainerExecutionException e) {
      // 命令执行异常，说明容器不存在
      LOG.debug("Container Status: {} ContainerId: {}",
          DockerContainerStatus.NONEXISTENT.getName(), containerId);
      return DockerContainerStatus.NONEXISTENT;
    }
  }

  /**
   * 从状态字符串解析得到Docker容器状态枚举
   * 匹配失败时返回UNKNOWN状态
   *
   * @param containerStatusStr docker inspect返回的状态字符串
   * @return 对应的容器状态枚举
   */
  public static DockerContainerStatus parseContainerStatus(
      String containerStatusStr) {
    DockerContainerStatus dockerContainerStatus;
    if (containerStatusStr == null) {
      dockerContainerStatus = DockerContainerStatus.UNKNOWN;
    } else if (containerStatusStr
        .equals(DockerContainerStatus.CREATED.getName())) {
      dockerContainerStatus = DockerContainerStatus.CREATED;
    } else if (containerStatusStr
        .equals(DockerContainerStatus.RUNNING.getName())) {
      dockerContainerStatus = DockerContainerStatus.RUNNING;
    } else if (containerStatusStr
        .equals(DockerContainerStatus.STOPPED.getName())) {
      dockerContainerStatus = DockerContainerStatus.STOPPED;
    } else if (containerStatusStr
        .equals(DockerContainerStatus.RESTARTING.getName())) {
      dockerContainerStatus = DockerContainerStatus.RESTARTING;
    } else if (containerStatusStr
        .equals(DockerContainerStatus.REMOVING.getName())) {
      dockerContainerStatus = DockerContainerStatus.REMOVING;
    } else if (containerStatusStr
        .equals(DockerContainerStatus.DEAD.getName())) {
      dockerContainerStatus = DockerContainerStatus.DEAD;
    } else if (containerStatusStr
        .equals(DockerContainerStatus.EXITED.getName())) {
      dockerContainerStatus = DockerContainerStatus.EXITED;
    } else if (containerStatusStr
        .equals(DockerContainerStatus.NONEXISTENT.getName())) {
      dockerContainerStatus = DockerContainerStatus.NONEXISTENT;
    } else {
      dockerContainerStatus = DockerContainerStatus.UNKNOWN;
    }
    return dockerContainerStatus;
  }

  /**
   * 执行docker inspect命令获取Docker容器状态字符串
   *
   * @param containerId                 目标容器ID
   * @param privilegedOperationExecutor 特权操作执行器
   * @param nmContext                  NodeManager上下文对象
   * @return 容器状态字符串
   * @throws ContainerExecutionException 命令执行失败时抛出异常
   */
  private static String executeStatusCommand(String containerId,
      PrivilegedOperationExecutor privilegedOperationExecutor,
      Context nmContext)
      throws ContainerExecutionException {
    // 构造docker inspect命令，并指定只获取状态字段
    DockerInspectCommand dockerInspectCommand =
        new DockerInspectCommand(containerId).getContainerStatus();
    try {
      // 执行命令并返回结果，禁用失败日志避免不存在容器产生冗余日志
      return DockerCommandExecutor.executeDockerCommand(dockerInspectCommand,
          containerId, null, privilegedOperationExecutor, true, nmContext);
    } catch (ContainerExecutionException e) {
      throw new ContainerExecutionException(e);
    }
  }

  /**
   * 判断容器是否处于可停止状态
   * 只有运行中或重启中的容器可以被停止
   *
   * @param containerStatus   当前容器状态
   * @return                  true表示可停止，false表示不可停止
   */
  public static boolean isStoppable(DockerContainerStatus containerStatus) {
    if (containerStatus.equals(DockerContainerStatus.RUNNING)
        || containerStatus.equals(DockerContainerStatus.RESTARTING)) {
      return true;
    }
    return false;
  }

  /**
   * 判断容器是否处于可杀死状态
   * 可杀死状态与可停止状态一致
   *
   * @param containerStatus   当前容器状态
   * @return                  true表示可杀死，false表示不可杀死
   */
  public static boolean isKillable(DockerContainerStatus containerStatus) {
    return isStoppable(containerStatus);
  }

  /**
   * 判断容器是否处于可删除状态
   * 不存在、未知、正在删除、运行中容器不可删除
   *
   * @param containerStatus   当前容器状态
   * @return                  true表示可删除，false表示不可删除
   */
  public static boolean isRemovable(DockerContainerStatus containerStatus) {
    return !containerStatus.equals(DockerContainerStatus.NONEXISTENT)
        && !containerStatus.equals(DockerContainerStatus.UNKNOWN)
        && !containerStatus.equals(DockerContainerStatus.REMOVING)
        && !containerStatus.equals(DockerContainerStatus.RUNNING);
  }

  /**
   * 判断容器是否处于可启动状态
   * 已退出或已停止容器可以被重新启动
   *
   * @param containerStatus   当前容器状态
   * @return                  true表示可启动，false表示不可启动
   */
  public static boolean isStartable(DockerContainerStatus containerStatus) {
    if (containerStatus.equals(DockerContainerStatus.EXITED)
        || containerStatus.equals(DockerContainerStatus.STOPPED)) {
      return true;
    }
    return false;
  }
}