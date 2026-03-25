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
package org.apache.hadoop.mapreduce.v2.app.rm.preemption;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 结合任务检查点机制实现的ApplicationMaster抢占策略，负责响应RM的抢占请求，选择并标记需要被抢占的任务容器。
 * 同时统一维护任务检查点信息，协调抢占操作和检查点管理，仅支持Reduce任务的抢占和检查点。
 * 对严格抢占请求直接处理指定容器，对可协商抢占请求按反向分配顺序选择足够满足资源需求的Reduce容器抢占。
 */
public class CheckpointAMPreemptionPolicy implements AMPreemptionPolicy {

  // 待抢占的任务尝试集合
  private final Set<TaskAttemptId> toBePreempted;

  // 已统计过的抢占任务尝试集合，用于避免重复计数
  private final Set<TaskAttemptId> countedPreemptions;

  // 任务ID对应检查点ID的映射表，维护已完成检查点
  private final Map<TaskId,TaskCheckpointID> checkpoints;

  // 正在进行中的可抢占任务尝试对应资源，用于扣除已发起的抢占资源
  private final Map<TaskAttemptId,Resource> pendingFlexiblePreemptions;

  @SuppressWarnings("rawtypes")
  private EventHandler eventHandler;

  static final Logger LOG = LoggerFactory
      .getLogger(CheckpointAMPreemptionPolicy.class);

  public CheckpointAMPreemptionPolicy() {
    this(Collections.synchronizedSet(new HashSet<TaskAttemptId>()),
         Collections.synchronizedSet(new HashSet<TaskAttemptId>()),
         Collections.synchronizedMap(new HashMap<TaskId,TaskCheckpointID>()),
         Collections.synchronizedMap(new HashMap<TaskAttemptId,Resource>()));
  }

  CheckpointAMPreemptionPolicy(Set<TaskAttemptId> toBePreempted,
      Set<TaskAttemptId> countedPreemptions,
      Map<TaskId,TaskCheckpointID> checkpoints,
      Map<TaskAttemptId,Resource> pendingFlexiblePreemptions) {
    this.toBePreempted = toBePreempted;
    this.countedPreemptions = countedPreemptions;
    this.checkpoints = checkpoints;
    this.pendingFlexiblePreemptions = pendingFlexiblePreemptions;
  }

  @Override
  /**
   * 初始化抢占策略，获取应用上下文的事件处理器
   * @param context 应用上下文
   */
  public void init(AppContext context) {
    this.eventHandler = context.getEventHandler();
  }

  @Override
  /**
   * 处理ResourceManager发送的抢占请求，分别处理严格抢占和可协商抢占
   * @param ctxt 抢占策略上下文，提供容器和任务映射查询
   * @param preemptionRequests RM发送的抢占请求消息
   */
  public void preempt(Context ctxt, PreemptionMessage preemptionRequests) {

    if (preemptionRequests != null) {

      // 处理不可协商的严格抢占请求

      StrictPreemptionContract cStrict = preemptionRequests.getStrictContract();
      if (cStrict != null
          && cStrict.getContainers() != null
          && cStrict.getContainers().size() > 0) {
        LOG.info("strict preemption :" +
            preemptionRequests.getStrictContract().getContainers().size() +
            " containers to kill");

        // 遍历处理每个需要被抢占的容器
        for (PreemptionContainer c :
            preemptionRequests.getStrictContract().getContainers()) {
          ContainerId reqCont = c.getId();
          TaskAttemptId reqTask = ctxt.getTaskAttempt(reqCont);
          if (reqTask != null) {
            // 仅抢占Reduce任务容器，不抢占Map任务
            if (org.apache.hadoop.mapreduce.v2.api.records.TaskType.REDUCE
                .equals(reqTask.getTaskId().getTaskType())) {
              toBePreempted.add(reqTask);
              LOG.info("preempting " + reqCont + " running task:" + reqTask);
            } else {
              LOG.info("NOT preempting " + reqCont + " running task:" + reqTask);
            }
          }
        }
      }

      // 处理可协商的灵活抢占请求
      PreemptionContract cNegot = preemptionRequests.getContract();
      if (cNegot != null
          && cNegot.getResourceRequest() != null
          && cNegot.getResourceRequest().size() > 0
          && cNegot.getContainers() != null
          && cNegot.getContainers().size() > 0) {

        LOG.info("negotiable preemption :" +
            preemptionRequests.getContract().getResourceRequest().size() +
            " resourceReq, " +
            preemptionRequests.getContract().getContainers().size() +
            " containers");
        // 当前仅支持Reduce任务检查点，只抢占Reduce容器
        List<PreemptionResourceRequest> reqResources =
          preemptionRequests.getContract().getResourceRequest();

        // 计算当前正在处理中的抢占资源总量，用于从本次请求中扣除
        int pendingPreemptionRam = 0;
        int pendingPreemptionCores = 0;
        for (Resource r : pendingFlexiblePreemptions.values()) {
          pendingPreemptionRam += r.getMemorySize();
          pendingPreemptionCores += r.getVirtualCores();
        }

        // 根据正在处理中的抢占，扣除本次需要抢占的资源量
        for (PreemptionResourceRequest rr : reqResources) {
          ResourceRequest reqRsrc = rr.getResourceRequest();
          if (!ResourceRequest.ANY.equals(reqRsrc.getResourceName())) {
            // 当前仅处理聚合请求，忽略位置相关请求
            continue;
          }

          LOG.info("ResourceRequest:" + reqRsrc);
          int reqCont = reqRsrc.getNumContainers();
          long reqMem = reqRsrc.getCapability().getMemorySize();
          long totalMemoryToRelease = reqCont * reqMem;
          int reqCores = reqRsrc.getCapability().getVirtualCores();
          int totalCoresToRelease = reqCont * reqCores;

          // 扣除已经在处理中的抢占内存
          if (pendingPreemptionRam > 0) {
            totalMemoryToRelease -= pendingPreemptionRam;
            pendingPreemptionRam -= totalMemoryToRelease;
          }
          // 扣除已经在处理中的抢占核数
          if (pendingPreemptionCores > 0) {
            totalCoresToRelease -= pendingPreemptionCores;
            pendingPreemptionCores -= totalCoresToRelease;
          }

          // 获取所有运行中的Reduce容器，按分配逆序排序（后分配先抢占）
          List<Container> listOfCont = ctxt.getContainers(TaskType.REDUCE);
          Collections.sort(listOfCont, new Comparator<Container>() {
            @Override
            public int compare(final Container o1, final Container o2) {
              return o2.getId().compareTo(o1.getId());
            }
          });

          // 依次选择容器抢占，直到满足资源需求
          for (Container cont : listOfCont) {
            if (totalMemoryToRelease <= 0 && totalCoresToRelease<=0) {
              // 资源需求已满足，退出选择
              break;
            }
            TaskAttemptId reduceId = ctxt.getTaskAttempt(cont.getId());
            int cMem = (int) cont.getResource().getMemorySize();
            int cCores = cont.getResource().getVirtualCores();

            if (!toBePreempted.contains(reduceId)) {
              totalMemoryToRelease -= cMem;
              totalCoresToRelease -= cCores;
              toBePreempted.add(reduceId);
              pendingFlexiblePreemptions.put(reduceId, cont.getResource());
            }
            LOG.info("ResourceRequest:" + reqRsrc + " satisfied preempting "
                + reduceId);
          }
        }
      }
    }
  }

  @Override
  /**
   * 处理容器失败事件，清理对应任务的抢占标记和检查点
   * @param attemptID 失败的任务尝试ID
   */
  public void handleFailedContainer(TaskAttemptId attemptID) {
    toBePreempted.remove(attemptID);
    checkpoints.remove(attemptID.getTaskId());
  }

  @Override
  /**
   * 处理容器完成事件，清理对应任务的抢占标记和待处理抢占记录
   * @param attemptID 完成的任务尝试ID
   */
  public void handleCompletedContainer(TaskAttemptId attemptID){
    LOG.info(" task completed:" + attemptID);
    toBePreempted.remove(attemptID);
    pendingFlexiblePreemptions.remove(attemptID);
  }

  @Override
  /**
   * 判断指定任务尝试是否需要被抢占，如果需要则更新计数器
   * @param yarnAttemptID 待检查的任务尝试ID
   * @return true表示需要抢占，false表示不需要
   */
  public boolean isPreempted(TaskAttemptId yarnAttemptID) {
    if (toBePreempted.contains(yarnAttemptID)) {
      updatePreemptionCounters(yarnAttemptID);
      return true;
    }
    return false;
  }

  @Override
  /**
   * 处理成功抢占的报告，本策略不处理该事件
   * @param taskAttemptID 已成功抢占的任务尝试ID
   */
  public void reportSuccessfulPreemption(TaskAttemptId taskAttemptID) {
    // ignore
  }

  @Override
  /**
   * 获取指定任务的检查点ID
   * @param taskId 任务ID
   * @return 任务对应的检查点ID，如果不存在则返回null
   */
  public TaskCheckpointID getCheckpointID(TaskId taskId) {
    return checkpoints.get(taskId);
  }

  @Override
  /**
   * 设置指定任务的检查点ID，并更新检查点相关计数器
   * @param taskId 任务ID
   * @param cid 检查点ID
   */
  public void setCheckpointID(TaskId taskId, TaskCheckpointID cid) {
    checkpoints.put(taskId, cid);
    if (cid != null) {
      updateCheckpointCounters(taskId, cid);
    }
  }

  @SuppressWarnings({ "unchecked" })
  /**
   * 更新检查点相关作业计数器，包括检查点数量、检查点字节数、检查点耗时
   * @param taskId 生成检查点的任务ID
   * @param cid 新生成的检查点ID
   */
  private void updateCheckpointCounters(TaskId taskId, TaskCheckpointID cid) {
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskId.getJobId());
    jce.addCounterUpdate(JobCounter.CHECKPOINTS, 1);
    eventHandler.handle(jce);
    jce = new JobCounterUpdateEvent(taskId.getJobId());
    jce.addCounterUpdate(JobCounter.CHECKPOINT_BYTES, cid.getCheckpointBytes());
    eventHandler.handle(jce);
    jce = new JobCounterUpdateEvent(taskId.getJobId());
    jce.addCounterUpdate(JobCounter.CHECKPOINT_TIME, cid.getCheckpointTime());
    eventHandler.handle(jce);

  }

  @SuppressWarnings({ "unchecked" })
  /**
   * 更新抢占请求计数器，避免同一个任务重复计数
   * @param yarnAttemptID 被抢占的任务尝试ID
   */
  private void updatePreemptionCounters(TaskAttemptId yarnAttemptID) {
    if (!countedPreemptions.contains(yarnAttemptID)) {
      countedPreemptions.add(yarnAttemptID);
      JobCounterUpdateEvent jce = new JobCounterUpdateEvent(yarnAttemptID
          .getTaskId().getJobId());
      jce.addCounterUpdate(JobCounter.TASKS_REQ_PREEMPT, 1);
      eventHandler.handle(jce);
    }
  }

}