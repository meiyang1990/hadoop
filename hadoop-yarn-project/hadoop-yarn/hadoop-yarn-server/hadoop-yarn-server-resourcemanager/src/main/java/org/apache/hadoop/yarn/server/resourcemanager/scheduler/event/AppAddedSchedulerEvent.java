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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;

/**
 * 应用添加到调度器事件，YARN调度器处理新应用提交时的事件对象
 * 封装新应用提交所需的全部上下文信息，供调度器处理应用加入队列
 */
public class AppAddedSchedulerEvent extends SchedulerEvent {

  private final ApplicationId applicationId;
  private final String queue;
  private final String user;
  private final ReservationId reservationID;
  private final boolean isAppRecovering;
  private final Priority appPriority;
  private final ApplicationPlacementContext placementContext;
  private boolean unmanagedAM = false;

  /**
   * 构造方法，创建基本的应用添加事件
   */
  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user) {
    this(applicationId, queue, user, false, null, Priority.newInstance(0),
        null);
  }

  /**
   * 构造方法，创建带放置上下文的应用添加事件
   */
  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, ApplicationPlacementContext placementContext) {
    this(applicationId, queue, user, false, null, Priority.newInstance(0),
        placementContext);
  }

  /**
   * 构造方法，创建带预留ID和优先级的应用添加事件
   */
  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, ReservationId reservationID, Priority appPriority) {
    this(applicationId, queue, user, false, reservationID, appPriority, null);
  }

  /**
   * 构造方法，从提交上下文创建应用添加事件，支持应用恢复场景
   */
  public AppAddedSchedulerEvent(String user,
      ApplicationSubmissionContext submissionContext, boolean isAppRecovering,
      Priority appPriority) {
    this(submissionContext.getApplicationId(), submissionContext.getQueue(),
        user, isAppRecovering, submissionContext.getReservationID(),
        appPriority, null);
    this.unmanagedAM = submissionContext.getUnmanagedAM();
  }

  /**
   * 构造方法，从提交上下文创建应用添加事件，带放置上下文，支持应用恢复场景
   */
  public AppAddedSchedulerEvent(String user,
      ApplicationSubmissionContext submissionContext, boolean isAppRecovering,
      Priority appPriority, ApplicationPlacementContext placementContext) {
    this(submissionContext.getApplicationId(), submissionContext.getQueue(),
        user, isAppRecovering, submissionContext.getReservationID(),
        appPriority, placementContext);
    this.unmanagedAM = submissionContext.getUnmanagedAM();
  }

  /**
   * 全参数构造方法，创建完整的应用添加事件
   */
  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, boolean isAppRecovering, ReservationId reservationID,
      Priority appPriority, ApplicationPlacementContext placementContext) {
    super(SchedulerEventType.APP_ADDED);
    this.applicationId = applicationId;
    this.queue = queue;
    this.user = user;
    this.reservationID = reservationID;
    this.isAppRecovering = isAppRecovering;
    this.appPriority = appPriority;
    this.placementContext = placementContext;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public String getQueue() {
    return queue;
  }

  public String getUser() {
    return user;
  }

  public boolean getIsAppRecovering() {
    return isAppRecovering;
  }

  public ReservationId getReservationID() {
    return reservationID;
  }

  public Priority getApplicatonPriority() {
    return appPriority;
  }

  public ApplicationPlacementContext getPlacementContext() {
    return placementContext;
  }

  public boolean isUnmanagedAM() {
    return unmanagedAM;
  }
}