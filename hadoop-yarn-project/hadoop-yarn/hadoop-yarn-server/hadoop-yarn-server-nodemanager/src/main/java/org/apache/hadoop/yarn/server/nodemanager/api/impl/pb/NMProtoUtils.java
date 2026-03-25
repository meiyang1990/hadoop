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
package org.apache.hadoop.yarn.server.nodemanager.api.impl.pb;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.recovery.DeletionTaskRecoveryInfo;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTaskType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DockerContainerDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * NodeManager Protobuf转换工具类，负责将Protobuf格式的持久化数据转换为NodeManager内部对象。
 * 主要用于删除任务恢复场景，将序列化的删除任务信息反序列化为内存对象。
 */
public final class NMProtoUtils {

  /** 日志处理器 */
  private static final Logger LOG =
       LoggerFactory.getLogger(NMProtoUtils.class);

  /** 工具类禁止实例化 */
  private NMProtoUtils() { }

  /**
   * 将Protobuf格式的删除任务转换为NodeManager内存中的DeletionTask对象。
   * 根据任务类型分发到对应具体类型的转换方法，默认兼容处理为文件删除任务。
   *
   * @param proto             Protobuf格式的删除任务数据
   * @param deletionService   删除任务所属的删除服务实例
   * @return 转换完成的DeletionTask具体实例
   */
  public static DeletionTask convertProtoToDeletionTask(
      DeletionServiceDeleteTaskProto proto, DeletionService deletionService) {
    int taskId = proto.getId();
    if (proto.hasTaskType() && proto.getTaskType() != null) {
      if (proto.getTaskType().equals(DeletionTaskType.FILE.name())) {
        LOG.debug("Converting recovered FileDeletionTask");
        return convertProtoToFileDeletionTask(proto, deletionService, taskId);
      } else if (proto.getTaskType().equals(
          DeletionTaskType.DOCKER_CONTAINER.name())) {
        LOG.debug("Converting recovered DockerContainerDeletionTask");
        return convertProtoToDockerContainerDeletionTask(proto, deletionService,
            taskId);
      }
    }
    LOG.debug("Unable to get task type, trying FileDeletionTask");
    return convertProtoToFileDeletionTask(proto, deletionService, taskId);
  }

  /**
   * 将Protobuf格式数据转换为FileDeletionTask文件删除任务实例。
   *
   * @param proto Protobuf格式的文件删除任务数据
   * @param deletionService 删除服务实例
   * @param taskId 删除任务ID
   * @return 转换完成的文件删除任务实例
   */
  public static FileDeletionTask convertProtoToFileDeletionTask(
      DeletionServiceDeleteTaskProto proto, DeletionService deletionService,
      int taskId) {
    // 获取任务所属用户，没有则为空
    String user = proto.hasUser() ? proto.getUser() : null;
    Path subdir = null;
    // 解析子目录路径
    if (proto.hasSubdir()) {
      subdir = new Path(proto.getSubdir());
    }
    List<Path> basePaths = null;
    List<String> basedirs = proto.getBasedirsList();
    // 转换待删除基础路径列表
    if (basedirs != null && basedirs.size() > 0) {
      basePaths = new ArrayList<>(basedirs.size());
      for (String basedir : basedirs) {
        basePaths.add(new Path(basedir));
      }
    }
    return new FileDeletionTask(taskId, deletionService, user, subdir,
        basePaths);
  }

  /**
   * 将Protobuf格式数据转换为DockerContainerDeletionTask容器删除任务实例。
   *
   * @param proto Protobuf格式的Docker容器删除任务数据
   * @param deletionService 删除服务实例
   * @param taskId 删除任务ID
   * @return 转换完成的Docker容器删除任务实例
   */
  public static DockerContainerDeletionTask
      convertProtoToDockerContainerDeletionTask(
      DeletionServiceDeleteTaskProto proto, DeletionService deletionService,
      int taskId) {
    // 获取任务所属用户，没有则为空
    String user = proto.hasUser() ? proto.getUser() : null;
    // 获取待删除Docker容器ID，没有则为空
    String containerId =
        proto.hasDockerContainerId() ? proto.getDockerContainerId() : null;
    return new DockerContainerDeletionTask(taskId, deletionService, user,
        containerId);
  }

  /**
   * 将Protobuf格式的删除任务转换为DeletionTaskRecoveryInfo删除任务恢复信息。
   * 用于NodeManager重启后恢复删除任务队列，包含任务依赖和时间戳信息。
   *
   * @param proto Protobuf格式的删除任务数据
   * @param deletionService 删除服务实例
   * @return 转换完成的删除任务恢复信息对象
   */
  public static DeletionTaskRecoveryInfo convertProtoToDeletionTaskRecoveryInfo(
      DeletionServiceDeleteTaskProto proto, DeletionService deletionService) {
    // 先转换得到删除任务本身
    DeletionTask deletionTask =
        NMProtoUtils.convertProtoToDeletionTask(proto, deletionService);
    List<Integer> successorTaskIds = new ArrayList<>();
    // 读取后继任务ID列表
    if (proto.getSuccessorIdsList() != null &&
        !proto.getSuccessorIdsList().isEmpty()) {
      successorTaskIds = proto.getSuccessorIdsList();
    }
    // 获取删除任务创建时间戳
    long deletionTimestamp = proto.getDeletionTime();
    return new DeletionTaskRecoveryInfo(deletionTask, successorTaskIds,
        deletionTimestamp);
  }
}