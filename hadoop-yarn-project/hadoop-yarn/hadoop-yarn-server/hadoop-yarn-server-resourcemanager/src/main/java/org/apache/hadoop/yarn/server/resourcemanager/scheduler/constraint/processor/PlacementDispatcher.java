// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutputCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingRequestWithPlacementAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 约束放置算法调度器，负责初始化约束放置算法、分发调度请求、收集算法输出结果，
 * 为YARN的容器放置约束处理提供异步执行能力，隔离算法执行与调度主线程。
 */
class PlacementDispatcher implements
    ConstraintPlacementAlgorithmOutputCollector {

  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementDispatcher.class);
  // 实际执行放置逻辑的约束放置算法实例
  private ConstraintPlacementAlgorithm algorithm;
  // 用于异步执行放置算法的线程池
  private ExecutorService algorithmThreadPool;

  // 按应用ID存储已完成放置的调度请求列表，线程安全
  private Map<ApplicationId, List<PlacedSchedulingRequest>>
      placedRequests = new ConcurrentHashMap<>();
  // 按应用ID存储被拒绝的调度请求列表，线程安全
  private Map<ApplicationId, List<SchedulingRequestWithPlacementAttempt>>
      rejectedRequests = new ConcurrentHashMap<>();

  /**
   * 初始化放置调度器，完成算法初始化和线程池创建。
   * @param rmContext YARN RM上下文对象
   * @param placementAlgorithm 待使用的约束放置算法实例
   * @param poolSize 算法执行线程池大小
   */
  public void init(RMContext rmContext,
      ConstraintPlacementAlgorithm placementAlgorithm, int poolSize) {
    LOG.info("Initializing Constraint Placement Planner:");
    this.algorithm = placementAlgorithm;
    this.algorithm.init(rmContext);
    this.algorithmThreadPool = Executors.newFixedThreadPool(poolSize);
  }

  /**
   * 异步分发批量放置请求到线程池执行。
   * @param batchedRequests 批量待放置的调度请求
   */
  void dispatch(final BatchedRequests batchedRequests) {
    final ConstraintPlacementAlgorithmOutputCollector collector = this;
    // 构造异步放置任务
    Runnable placingTask = () -> {
      LOG.debug("Got [{}] requests to place from application [{}].. " +
              "Attempt count [{}]",
          batchedRequests.getSchedulingRequests().size(),
          batchedRequests.getApplicationId(),
          batchedRequests.getPlacementAttempt());
      // 调用算法执行放置
      algorithm.place(batchedRequests, collector);
    };
    // 提交任务到线程池
    this.algorithmThreadPool.submit(placingTask);
  }

  /**
   * 取出指定应用所有已完成放置的请求，取出后清空缓存。
   * @param applicationId 应用ID
   * @return 已放置的调度请求列表，无结果返回空列表
   */
  public List<PlacedSchedulingRequest> pullPlacedRequests(
      ApplicationId applicationId) {
    List<PlacedSchedulingRequest> placedReqs =
        this.placedRequests.get(applicationId);
    if (placedReqs != null && !placedReqs.isEmpty()) {
      List<PlacedSchedulingRequest> retList = new ArrayList<>();
      // 同步保证线程安全，避免并发拉取时数据丢失
      synchronized (placedReqs) {
        if (placedReqs.size() > 0) {
          retList.addAll(placedReqs);
          placedReqs.clear();
        }
      }
      return retList;
    }
    return Collections.emptyList();
  }

  /**
   * 取出指定应用所有被拒绝的放置请求，取出后清空缓存。
   * @param applicationId 应用ID
   * @return 被拒绝的调度请求列表，无结果返回空列表
   */
  public List<SchedulingRequestWithPlacementAttempt> pullRejectedRequests(
      ApplicationId applicationId) {
    List<SchedulingRequestWithPlacementAttempt> rejectedReqs =
        this.rejectedRequests.get(applicationId);
    if (rejectedReqs != null && !rejectedReqs.isEmpty()) {
      List<SchedulingRequestWithPlacementAttempt> retList = new ArrayList<>();
      // 同步保证线程安全，避免并发拉取时数据丢失
      synchronized (rejectedReqs) {
        if (rejectedReqs.size() > 0) {
          retList.addAll(rejectedReqs);
          rejectedReqs.clear();
        }
      }
      return retList;
    }
    return Collections.emptyList();
  }

  /**
   * 清理指定应用的所有放置状态缓存，应用完成后调用释放内存。
   * @param applicationId 应用ID
   */
  void clearApplicationState(ApplicationId applicationId) {
    placedRequests.remove(applicationId);
    rejectedRequests.remove(applicationId);
  }

  @Override
  /**
   * 收集约束放置算法的输出结果，分类存储已放置和被拒绝的请求。
   * @param placement 算法输出的放置结果
   */
  public void collect(ConstraintPlacementAlgorithmOutput placement) {
    // 处理已放置成功的请求
    if (!placement.getPlacedRequests().isEmpty()) {
      List<PlacedSchedulingRequest> processed =
          placedRequests.computeIfAbsent(
              placement.getApplicationId(), k -> new ArrayList<>());
      synchronized (processed) {
        LOG.debug(
            "Planning Algorithm has placed for application [{}]" +
                " the following [{}]", placement.getApplicationId(),
            placement.getPlacedRequests());
        for (PlacedSchedulingRequest esr :
            placement.getPlacedRequests()) {
          processed.add(esr);
        }
      }
    }
    // 处理被放置算法拒绝的请求
    if (!placement.getRejectedRequests().isEmpty()) {
      List<SchedulingRequestWithPlacementAttempt> rejected =
          rejectedRequests.computeIfAbsent(
              placement.getApplicationId(), k -> new ArrayList());
      LOG.warn(
          "Planning Algorithm has rejected for application [{}]" +
              " the following [{}]", placement.getApplicationId(),
          placement.getRejectedRequests());
      synchronized (rejected) {
        rejected.addAll(placement.getRejectedRequests());
      }
    }
  }
}