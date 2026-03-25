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
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 抽象删除任务基类，定义了删除任务的通用结构和依赖管理逻辑，提交给{@link DeletionService}执行清理操作。
 */
public abstract class DeletionTask implements Runnable {

  static final Logger LOG =
       LoggerFactory.getLogger(DeletionTask.class);

  public static final int INVALID_TASK_ID = -1;

  private int taskId;
  private String user;
  private DeletionTaskType deletionTaskType;
  private DeletionService deletionService;
  private final AtomicInteger numberOfPendingPredecessorTasks;
  private final Set<DeletionTask> successorTaskSet;
  // By default all tasks will start as success=true; however if any of
  // the dependent task fails then it will be marked as false in
  // deletionTaskFinished().
  private boolean success;

  /**
   * 构造删除任务，使用默认的依赖计数和后继任务集合。
   *
   * @param taskId              删除任务ID，恢复场景下可传入已有ID
   * @param deletionService     所属删除服务
   * @param user                本次删除对应用户
   * @param deletionTaskType    删除任务类型
   */
  public DeletionTask(int taskId, DeletionService deletionService, String user,
      DeletionTaskType deletionTaskType) {
    this(taskId, deletionService, user, new AtomicInteger(0),
        new HashSet<DeletionTask>(), deletionTaskType);
  }

  /**
   * 完整构造删除任务，允许传入自定义的依赖计数和后继任务集合（用于恢复场景）。
   *
   * @param taskId              删除任务ID，恢复场景下可传入已有ID
   * @param deletionService     所属删除服务
   * @param user                本次删除对应用户
   * @param numberOfPendingPredecessorTasks 未完成前驱任务计数
   * @param successorTaskSet    后继任务集合
   * @param deletionTaskType    删除任务类型
   */
  public DeletionTask(int taskId, DeletionService deletionService, String user,
      AtomicInteger numberOfPendingPredecessorTasks,
      Set<DeletionTask> successorTaskSet, DeletionTaskType deletionTaskType) {
    this.taskId = taskId;
    this.deletionService = deletionService;
    this.user = user;
    this.numberOfPendingPredecessorTasks = numberOfPendingPredecessorTasks;
    this.successorTaskSet = successorTaskSet;
    this.deletionTaskType = deletionTaskType;
    success = true;
  }

  /**
   * 获取删除任务ID。
   *
   * @return 任务ID
   */
  public int getTaskId() {
    return taskId;
  }

  /**
   * 设置删除任务ID。
   *
   * @param taskId 任务ID
   */
  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  /**
   * 获取本次删除对应用户。
   *
   * @return 用户名
   */
  public String getUser() {
    return user;
  }

  /**
   * 获取所属删除服务。
   *
   * @return 删除服务实例
   */
  public DeletionService getDeletionService() {
    return deletionService;
  }

  /**
   * 获取删除任务类型。
   *
   * @return 删除任务类型
   */
  public DeletionTaskType getDeletionTaskType() {
    return deletionTaskType;
  }

  /**
   * 设置删除任务执行结果状态。
   *
   * @param success 执行是否成功
   */
  public synchronized void setSuccess(boolean success) {
    this.success = success;
  }

  /**
   * 获取删除任务执行结果状态。
   *
   * @return 执行是否成功
   */
  public synchronized boolean getSucess() {
    return this.success;
  }

  /**
   * 获取所有后继任务数组。
   *
   * @return 后继任务数组
   */
  public synchronized DeletionTask[] getSuccessorTasks() {
    DeletionTask[] successors = new DeletionTask[successorTaskSet.size()];
    return successorTaskSet.toArray(successors);
  }

  /**
   * 将删除任务转换为Protobuf格式，用于NM状态存储和恢复。
   *
   * @return 删除任务的Protobuf表示
   */
  public abstract DeletionServiceDeleteTaskProto convertDeletionTaskToProto();

  /**
   * 添加任务依赖，当前任务是后继任务的前驱，必须在删除任务提交前完成依赖定义。
   * 如果任务依赖关系为：任务2、任务3必须在任务1完成后执行，则任务2、任务3需要添加为任务1的后继任务。
   *
   * @param successorTask 依赖当前任务的后继删除任务
   */
  public synchronized void addDeletionTaskDependency(
      DeletionTask successorTask) {
    if (successorTaskSet.add(successorTask)) {
      successorTask.incrementAndGetPendingPredecessorTasks();
    }
  }

  /**
   * 增加未完成前驱任务计数并返回结果。
   *
   * @return 更新后的未完成前驱任务数
   */
  public int incrementAndGetPendingPredecessorTasks() {
    return numberOfPendingPredecessorTasks.incrementAndGet();
  }

  /**
   * 减少未完成前驱任务计数并返回结果。
   *
   * @return 更新后的未完成前驱任务数
   */
  public int decrementAndGetPendingPredecessorTasks() {
    return numberOfPendingPredecessorTasks.decrementAndGet();
  }

  /**
   * 当前删除任务完成后的处理逻辑：从状态存储删除任务记录，处理后继任务调度。
   * 触发场景：1) 当前任务执行完成；2) 前驱任务失败直接标记当前任务失败并触发完成处理
   */
  synchronized void deletionTaskFinished() {
    try {
      // 从NM状态存储中删除该任务记录
      NMStateStoreService stateStore = deletionService.getStateStore();
      stateStore.removeDeletionTask(taskId);
    } catch (IOException e) {
      LOG.error("Unable to remove deletion task " + taskId
          + " from state store", e);
    }
    // 遍历所有后继任务，更新依赖计数并调度可执行任务
    Iterator<DeletionTask> successorTaskI = this.successorTaskSet.iterator();
    while (successorTaskI.hasNext()) {
      DeletionTask successorTask = successorTaskI.next();
      // 如果当前任务失败，标记所有后继任务失败
      if (!success) {
        successorTask.setSuccess(success);
      }
      // 后继任务未完成前驱计数减一
      int count = successorTask.decrementAndGetPendingPredecessorTasks();
      // 所有前驱都已完成，调度后继任务
      if (count == 0) {
        if (successorTask.getSucess()) {
          // 所有前驱成功，提交后继任务执行
          successorTask.deletionService.delete(successorTask);
        } else {
          // 存在前驱失败，直接触发后继任务完成流程
          successorTask.deletionTaskFinished();
        }
      }
    }
  }

  /**
   * 获取填充了基础属性的Protobuf Builder，供子类实现convertDeletionTaskToProto使用。
   *
   * @return 已填充基础属性的Protobuf Builder
   */
  DeletionServiceDeleteTaskProto.Builder getBaseDeletionTaskProtoBuilder() {
    DeletionServiceDeleteTaskProto.Builder builder =
        DeletionServiceDeleteTaskProto.newBuilder();
    // 填充任务ID
    builder.setId(getTaskId());
    // 填充用户信息
    if (getUser() != null) {
      builder.setUser(getUser());
    }
    // 计算删除时间（加上调试延迟）
    builder.setDeletionTime(System.currentTimeMillis() +
        TimeUnit.MILLISECONDS.convert(getDeletionService().getDebugDelay(),
            TimeUnit.SECONDS));
    // 填充所有后继任务ID
    for (DeletionTask successor : getSuccessorTasks()) {
      builder.addSuccessorIds(successor.getTaskId());
    }
    return builder;
  }
}