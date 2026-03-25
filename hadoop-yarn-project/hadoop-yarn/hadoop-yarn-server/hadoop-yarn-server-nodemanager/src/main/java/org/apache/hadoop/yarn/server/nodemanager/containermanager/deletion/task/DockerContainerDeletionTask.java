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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task;

import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;

/**
 * 负责清理Docker容器的删除任务，继承自DeletionTask实现Runnable接口。
 * 在YARN NodeManager上完成Docker容器退出后的资源清理工作。
 */
public class DockerContainerDeletionTask extends DeletionTask
    implements Runnable {
  private String containerId;

  /**
   * 构造Docker容器删除任务，使用默认无效任务ID。
   * @param deletionService 删除服务对象
   * @param user 任务所属用户
   * @param containerId 待删除Docker容器ID
   */
  public DockerContainerDeletionTask(DeletionService deletionService,
      String user, String containerId) {
    this(INVALID_TASK_ID, deletionService, user, containerId);
  }

  /**
   * 构造Docker容器删除任务，指定自定义任务ID。
   * @param taskId 删除任务ID
   * @param deletionService 删除服务对象
   * @param user 任务所属用户
   * @param containerId 待删除Docker容器ID
   */
  public DockerContainerDeletionTask(int taskId,
      DeletionService deletionService, String user, String containerId) {
    super(taskId, deletionService, user, DeletionTaskType.DOCKER_CONTAINER);
    this.containerId = containerId;
  }

  /**
   * 获取待删除Docker容器ID。
   * @return 待删除Docker容器ID
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * 执行Docker容器删除任务，调用容器执行器删除指定容器。
   */
  @Override
  public void run() {
    LOG.debug("Running DeletionTask : {}", this);
    // 获取Linux容器执行器实例
    LinuxContainerExecutor exec = ((LinuxContainerExecutor)
        getDeletionService().getContainerExecutor());
    // 调用执行器删除Docker容器
    exec.removeDockerContainer(containerId);
  }

  /**
   * 转换为字符串描述，包含任务ID和容器ID信息。
   * @return 任务描述字符串
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DockerContainerDeletionTask : ");
    sb.append("  id : ").append(this.getTaskId());
    sb.append("  containerId : ").append(this.containerId);
    return sb.toString().trim();
  }

  /**
   * 将当前任务转换为Protobuf格式，用于状态存储和恢复。
   * @return 任务的Protobuf表示
   */
  public DeletionServiceDeleteTaskProto convertDeletionTaskToProto() {
    // 获取基础任务的Proto构建器
    DeletionServiceDeleteTaskProto.Builder builder =
        getBaseDeletionTaskProtoBuilder();
    // 设置任务类型为Docker容器删除
    builder.setTaskType(DeletionTaskType.DOCKER_CONTAINER.name());
    // 如果容器ID不为空则设置到Proto中
    if (getContainerId() != null) {
      builder.setDockerContainerId(getContainerId());
    }
    // 构建并返回Proto对象
    return builder.build();
  }
}