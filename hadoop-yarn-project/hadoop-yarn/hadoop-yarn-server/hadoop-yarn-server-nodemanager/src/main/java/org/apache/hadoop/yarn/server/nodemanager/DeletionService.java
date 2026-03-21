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

package org.apache.hadoop.yarn.server.nodemanager;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.hadoop.yarn.server.nodemanager.recovery.RecoveryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.api.impl.pb.NMProtoUtils;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.recovery.DeletionTaskRecoveryInfo;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * NodeManager端异步文件删除服务，负责延迟批量处理容器、应用等资源的删除任务，
 * 支持任务依赖关系和NM重启后的任务恢复，避免阻塞主线程提升性能。
 */
public class DeletionService extends AbstractService {

  private static final Logger LOG =
       LoggerFactory.getLogger(DeletionService.class);

  // 删除任务调试延迟秒数
  private int debugDelay;
  // 容器执行器，用于执行实际的删除操作
  private final ContainerExecutor containerExecutor;
  // NM状态存储服务，用于持久化删除任务支持恢复
  private final NMStateStoreService stateStore;
  // 定时任务线程池，调度延迟删除任务
  private ScheduledThreadPoolExecutor sched;
  // 下一个删除任务ID生成器
  private AtomicInteger nextTaskId = new AtomicInteger(0);

  /**
   * 构造不支持恢复的删除服务，使用空状态存储。
   * @param exec 容器执行器
   */
  public DeletionService(ContainerExecutor exec) {
    this(exec, new NMNullStateStoreService());
  }

  /**
   * 构造带状态存储的删除服务，支持任务恢复。
   * @param containerExecutor 容器执行器
   * @param stateStore NM状态存储服务
   */
  public DeletionService(ContainerExecutor containerExecutor,
      NMStateStoreService stateStore) {
    super(DeletionService.class.getName());
    this.containerExecutor = containerExecutor;
    this.debugDelay = 0;
    this.stateStore = stateStore;
  }

  public int getDebugDelay() {
    return debugDelay;
  }

  public ContainerExecutor getContainerExecutor() {
    return containerExecutor;
  }

  public NMStateStoreService getStateStore() {
    return stateStore;
  }

  /**
   * 提交一个删除任务到服务，按配置延迟调度执行。
   * @param deletionTask 要执行的删除任务
   */
  public void delete(DeletionTask deletionTask) {
    if (debugDelay != -1) {
      LOG.debug("Scheduling DeletionTask (delay {}) : {}", debugDelay,
          deletionTask);
      // 将删除任务持久化到状态存储，支持恢复
      recordDeletionTaskInStateStore(deletionTask);
      // 调度任务延迟执行
      sched.schedule(deletionTask, debugDelay, TimeUnit.SECONDS);
    }
  }

  /**
   * 从NM状态存储恢复未完成的删除任务，重建任务依赖关系并调度就绪任务。
   * @param state 恢复后的删除服务状态
   * @throws IOException 恢复过程IO异常
   */
  private void recover(NMStateStoreService.RecoveredDeletionServiceState state)
      throws IOException {
    // 任务ID到恢复信息的映射
    Map<Integer, DeletionTaskRecoveryInfo> idToInfoMap =
        new HashMap<Integer, DeletionTaskRecoveryInfo>();
    // 记录所有作为后继任务的ID集合
    Set<Integer> successorTasks = new HashSet<Integer>();

    try (RecoveryIterator<DeletionServiceDeleteTaskProto> it =
             state.getIterator()) {
      while (it.hasNext()) {
        // 反序列化proto恢复任务
        DeletionServiceDeleteTaskProto proto = it.next();
        DeletionTaskRecoveryInfo info =
            NMProtoUtils.convertProtoToDeletionTaskRecoveryInfo(proto, this);
        // 建立ID映射
        idToInfoMap.put(info.getTask().getTaskId(), info);
        // 更新最大任务ID，保证新生成的ID不重复
        nextTaskId.set(Math.max(nextTaskId.get(), info.getTask().getTaskId()));
        // 收集所有后继任务ID
        successorTasks.addAll(info.getSuccessorTaskIds());
      }
    }

    // 恢复任务依赖关系，调度没有前驱的根任务
    final long now = System.currentTimeMillis();
    for (DeletionTaskRecoveryInfo info : idToInfoMap.values()) {
      // 重建当前任务对后继任务的依赖关系
      for (Integer successorId : info.getSuccessorTaskIds()){
        DeletionTaskRecoveryInfo successor = idToInfoMap.get(successorId);
        if (successor != null) {
          info.getTask().addDeletionTaskDependency(successor.getTask());
        } else {
          LOG.error("Unable to locate dependency task for deletion task "
              + info.getTask().getTaskId());
        }
      }
      // 如果当前任务不是任何任务的后继（根任务），直接调度执行
      if (!successorTasks.contains(info.getTask().getTaskId())) {
        // 计算剩余延迟时间
        long msecTilDeletion = info.getDeletionTimestamp() - now;
        sched.schedule(info.getTask(), msecTilDeletion, TimeUnit.MILLISECONDS);
      }
    }
  }

  /**
   * 生成唯一的删除任务ID，跳过无效ID值。
   * @return 新的唯一任务ID
   */
  private int generateTaskId() {
    // get the next ID but avoid an invalid ID
    int taskId = nextTaskId.incrementAndGet();
    while (taskId == DeletionTask.INVALID_TASK_ID) {
      taskId = nextTaskId.incrementAndGet();
    }
    return taskId;
  }

  /**
   * 将删除任务及其后继任务递归持久化到状态存储，用于NM重启恢复。
   * @param task 要持久化的删除任务
   */
  private void recordDeletionTaskInStateStore(DeletionTask task) {
    if (!stateStore.canRecover()) {
      // optimize the case where we aren't really recording
      return;
    }
    if (task.getTaskId() != DeletionTask.INVALID_TASK_ID) {
      return;  // task already recorded
    }

    // 分配唯一任务ID
    task.setTaskId(generateTaskId());

    // 先持久化后继任务，保证它们都生成好任务ID
    DeletionTask[] successors = task.getSuccessorTasks();
    for (DeletionTask successor : successors) {
      recordDeletionTaskInStateStore(successor);
    }

    try {
      // 将当前任务序列化存储
      stateStore.storeDeletionTask(task.getTaskId(),
          task.convertDeletionTaskToProto());
    } catch (IOException e) {
      LOG.error("Unable to store deletion task " + task.getTaskId(), e);
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 创建删除服务线程工厂，命名线程便于监控
    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("DeletionService #%d")
        .build();
    if (conf != null) {
      // 从配置读取删除线程数，使用默认值兜底
      sched = new HadoopScheduledThreadPoolExecutor(
          conf.getInt(YarnConfiguration.NM_DELETE_THREAD_COUNT,
              YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT), tf);
      // 读取调试延迟配置
      debugDelay = conf.getInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 0);
    } else {
      // 无配置时使用默认线程数创建线程池
      sched = new HadoopScheduledThreadPoolExecutor(
          YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT, tf);
    }
    // 关闭后不执行已延迟的任务，加快关闭速度
    sched.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    // 设置线程空闲保活时间
    sched.setKeepAliveTime(60L, SECONDS);
    // 如果支持恢复，从状态存储恢复未完成删除任务
    if (stateStore.canRecover()) {
      recover(stateStore.loadDeletionServiceState());
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    if (sched != null) {
      // 优雅关闭线程池，停止接收新任务
      sched.shutdown();
      boolean terminated = false;
      try {
        // 等待10秒让现有任务完成
        terminated = sched.awaitTermination(10, SECONDS);
      } catch (InterruptedException e) { }
      // 超时未终止则强制关闭
      if (!terminated) {
        sched.shutdownNow();
      }
    }
    super.serviceStop();
  }

  /**
   * 检查服务是否已经完全停止，仅用于单元测试。
   * @return true 服务完全停止，false 仍在运行
   */
  @Private
  public boolean isTerminated() {
    return getServiceState() == STATE.STOPPED && sched.isTerminated();
  }
}