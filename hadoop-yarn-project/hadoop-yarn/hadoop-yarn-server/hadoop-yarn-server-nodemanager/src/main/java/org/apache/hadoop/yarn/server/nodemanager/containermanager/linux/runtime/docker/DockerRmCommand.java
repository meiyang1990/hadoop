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

import java.util.Map;

/**
 * 封装Docker rm（删除容器）命令及其命令行参数，用于YARN NodeManager清理Docker容器
 */
public class DockerRmCommand extends DockerCommand {
  private static final String RM_COMMAND = "rm";
  private static final String CGROUP_HIERARCHY = "hierarchy";
  private String cGroupArg;

  /**
   * 构造Docker删除容器命令对象
   * @param containerName 要删除的Docker容器名称
   * @param hierarchy cgroup层级路径，用于清理cgroup
   */
  public DockerRmCommand(String containerName, String hierarchy) {
    super(RM_COMMAND);
    super.addCommandArguments("name", containerName);
    // 如果提供了有效的cgroup层级信息，添加到命令参数中
    if ((hierarchy != null) && !hierarchy.isEmpty()) {
      super.addCommandArguments(CGROUP_HIERARCHY, hierarchy);
      this.cGroupArg = hierarchy;
    }
  }

  /**
   * 准备特权操作，将删除Docker容器请求转换为可执行的特权操作
   * @param dockerCommand 当前docker命令对象
   * @param containerName Docker容器名称
   * @param env 环境变量映射
   * @param nmContext NodeManager上下文对象
   * @return 构建好的删除Docker容器特权操作
   */
  @Override
  public PrivilegedOperation preparePrivilegedOperation(
      DockerCommand dockerCommand, String containerName, Map<String,
      String> env, Context nmContext) {
    // 创建移除Docker容器类型的特权操作
    PrivilegedOperation dockerOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.REMOVE_DOCKER_CONTAINER);
    // 如果存在cgroup参数，添加到操作参数中用于后续清理
    if (this.cGroupArg != null) {
      dockerOp.appendArgs(cGroupArg);
    }
    // 添加容器名称参数
    dockerOp.appendArgs(containerName);
    return dockerOp;
  }
}