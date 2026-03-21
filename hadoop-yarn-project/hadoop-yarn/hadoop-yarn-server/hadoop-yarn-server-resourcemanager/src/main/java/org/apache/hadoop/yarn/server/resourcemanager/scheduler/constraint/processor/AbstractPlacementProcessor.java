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

import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * 所有容器放置处理器的抽象基类，实现ApplicationMasterServiceProcessor接口，
 * 负责处理应用提交时的放置约束注册、应用退出时的清理工作，为子类提供公共基础能力。
 */
public abstract class AbstractPlacementProcessor implements
    ApplicationMasterServiceProcessor{
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractPlacementProcessor.class);

  // 责任链中下一个AM服务处理器
  protected ApplicationMasterServiceProcessor nextAMSProcessor;
  // YARN调度器实例引用
  protected AbstractYarnScheduler scheduler;
  // 放置约束管理器，负责管理所有应用的放置约束
  private PlacementConstraintManager constraintManager;

  @Override
  public void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor) {
    this.nextAMSProcessor = nextProcessor;
    // 从RM上下文获取调度器实例
    this.scheduler =
        (AbstractYarnScheduler) ((RMContextImpl) amsContext).getScheduler();
    // 从RM上下文获取放置约束管理器实例
    this.constraintManager =
        ((RMContextImpl)amsContext).getPlacementConstraintManager();
  }

  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException {
    // 从注册请求中提取应用提交的放置约束
    Map<Set<String>, PlacementConstraint> appPlacementConstraints =
        request.getPlacementConstraints();
    // 处理提取到的放置约束，注册到约束管理器
    processPlacementConstraints(applicationAttemptId.getApplicationId(),
        appPlacementConstraints);
    // 将请求传递给责任链中下一个处理器继续处理
    nextAMSProcessor.registerApplicationMaster(applicationAttemptId, request,
        response);
  }

  /**
   * 处理应用提交的放置约束，将约束注册到约束管理器中
   * @param applicationId 应用ID
   * @param appPlacementConstraints 应用提交的放置约束映射
   */
  private void processPlacementConstraints(ApplicationId applicationId,
      Map<Set<String>, PlacementConstraint> appPlacementConstraints) {
    if (appPlacementConstraints != null && !appPlacementConstraints.isEmpty()) {
      LOG.info("Constraints added for application [{}] against tags [{}]",
          applicationId, appPlacementConstraints);
      // 向约束管理器注册当前应用的所有放置约束
      constraintManager.registerApplication(
          applicationId, appPlacementConstraints);
    }
  }

  @Override
  public void finishApplicationMaster(ApplicationAttemptId applicationAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {
    // 从约束管理器中注销当前应用，清理相关约束数据
    constraintManager.unregisterApplication(
        applicationAttemptId.getApplicationId());
    // 将请求传递给责任链中下一个处理器继续处理
    this.nextAMSProcessor.finishApplicationMaster(applicationAttemptId, request,
        response);
  }
}