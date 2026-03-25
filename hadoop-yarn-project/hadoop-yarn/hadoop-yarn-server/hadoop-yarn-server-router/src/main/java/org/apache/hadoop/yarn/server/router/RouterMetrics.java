// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * YARN Router联邦拦截器的指标收集类，维护Router处理各类请求的活动统计并通过Hadoop指标系统对外发布。
 */
@InterfaceAudience.Private
@Metrics(about = "Metrics for Router Federation Interceptor", context = "fedr")
public final class RouterMetrics {

  private static final MetricsInfo RECORD_INFO =
      info("RouterMetrics", "Router Federation Interceptor");
  // 标记指标是否已初始化，避免重复注册
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  // 各类操作失败计数指标
  @Metric("# of applications failed to be submitted")
  private MutableGaugeInt numAppsFailedSubmitted;
  @Metric("# of applications failed to be created")
  private MutableGaugeInt numAppsFailedCreated;
  @Metric("# of applications failed to be killed")
  private MutableGaugeInt numAppsFailedKilled;
  @Metric("# of application reports failed to be retrieved")
  private MutableGaugeInt numAppsFailedRetrieved;
  @Metric("# of multiple applications reports failed to be retrieved")
  private MutableGaugeInt numMultipleAppsFailedRetrieved;
  @Metric("# of getApplicationAttempts failed to be retrieved")
  private MutableGaugeInt numAppAttemptsFailedRetrieved;
  @Metric("# of getClusterMetrics failed to be retrieved")
  private MutableGaugeInt numGetClusterMetricsFailedRetrieved;
  @Metric("# of getClusterNodes failed to be retrieved")
  private MutableGaugeInt numGetClusterNodesFailedRetrieved;
  @Metric("# of getNodeToLabels failed to be retrieved")
  private MutableGaugeInt numGetNodeToLabelsFailedRetrieved;
  @Metric("# of getNodeToLabels failed to be retrieved")
  private MutableGaugeInt numGetLabelsToNodesFailedRetrieved;
  @Metric("# of getClusterNodeLabels failed to be retrieved")
  private MutableGaugeInt numGetClusterNodeLabelsFailedRetrieved;
  @Metric("# of getApplicationAttemptReports failed to be retrieved")
  private MutableGaugeInt numAppAttemptReportFailedRetrieved;
  @Metric("# of getQueueUserAcls failed to be retrieved")
  private MutableGaugeInt numGetQueueUserAclsFailedRetrieved;
  @Metric("# of getContainerReport failed to be retrieved")
  private MutableGaugeInt numGetContainerReportFailedRetrieved;
  @Metric("# of getContainers failed to be retrieved")
  private MutableGaugeInt numGetContainersFailedRetrieved;
  @Metric("# of listReservations failed to be retrieved")
  private MutableGaugeInt numListReservationsFailedRetrieved;
  @Metric("# of getResourceTypeInfo failed to be retrieved")
  private MutableGaugeInt numGetResourceTypeInfo;
  @Metric("# of failApplicationAttempt failed to be retrieved")
  private MutableGaugeInt numFailAppAttemptFailedRetrieved;
  @Metric("# of updateApplicationPriority failed to be retrieved")
  private MutableGaugeInt numUpdateAppPriorityFailedRetrieved;
  @Metric("# of updateApplicationPriority failed to be retrieved")
  private MutableGaugeInt numUpdateAppTimeoutsFailedRetrieved;
  @Metric("# of signalToContainer failed to be retrieved")
  private MutableGaugeInt numSignalToContainerFailedRetrieved;
  @Metric("# of getQueueInfo failed to be retrieved")
  private MutableGaugeInt numGetQueueInfoFailedRetrieved;
  @Metric("# of moveApplicationAcrossQueues failed to be retrieved")
  private MutableGaugeInt numMoveApplicationAcrossQueuesFailedRetrieved;
  @Metric("# of getResourceProfiles failed to be retrieved")
  private MutableGaugeInt numGetResourceProfilesFailedRetrieved;
  @Metric("# of getResourceProfile failed to be retrieved")
  private MutableGaugeInt numGetResourceProfileFailedRetrieved;
  @Metric("# of getAttributesToNodes failed to be retrieved")
  private MutableGaugeInt numGetAttributesToNodesFailedRetrieved;
  @Metric("# of getClusterNodeAttributes failed to be retrieved")
  private MutableGaugeInt numGetClusterNodeAttributesFailedRetrieved;
  @Metric("# of getNodesToAttributes failed to be retrieved")
  private MutableGaugeInt numGetNodesToAttributesFailedRetrieved;
  @Metric("# of getNewReservation failed to be retrieved")
  private MutableGaugeInt numGetNewReservationFailedRetrieved;
  @Metric("# of submitReservation failed to be retrieved")
  private MutableGaugeInt numSubmitReservationFailedRetrieved;
  @Metric("# of submitReservation failed to be retrieved")
  private MutableGaugeInt numUpdateReservationFailedRetrieved;
  @Metric("# of deleteReservation failed to be retrieved")
  private MutableGaugeInt numDeleteReservationFailedRetrieved;
  @Metric("# of listReservation failed to be retrieved")
  private MutableGaugeInt numListReservationFailedRetrieved;
  @Metric("# of getAppActivities failed to be retrieved")
  private MutableGaugeInt numGetAppActivitiesFailedRetrieved;
  @Metric("# of getAppStatistics failed to be retrieved")
  private MutableGaugeInt numGetAppStatisticsFailedRetrieved;
  @Metric("# of getAppPriority failed to be retrieved")
  private MutableGaugeInt numGetAppPriorityFailedRetrieved;
  @Metric("# of getAppQueue failed to be retrieved")
  private MutableGaugeInt numGetAppQueueFailedRetrieved;
  @Metric("# of updateAppQueue failed to be retrieved")
  private MutableGaugeInt numUpdateAppQueueFailedRetrieved;
  @Metric("# of getAppTimeout failed to be retrieved")
  private MutableGaugeInt numGetAppTimeoutFailedRetrieved;
  @Metric("# of getAppTimeouts failed to be retrieved")
  private MutableGaugeInt numGetAppTimeoutsFailedRetrieved;
  @Metric("# of refreshQueues failed to be retrieved")
  private MutableGaugeInt numRefreshQueuesFailedRetrieved;
  @Metric("# of getRMNodeLabels failed to be retrieved")
  private MutableGaugeInt numGetRMNodeLabelsFailedRetrieved;
  @Metric("# of checkUserAccessToQueue failed to be retrieved")
  private MutableGaugeInt numCheckUserAccessToQueueFailedRetrieved;
  @Metric("# of refreshNodes failed to be retrieved")
  private MutableGaugeInt numRefreshNodesFailedRetrieved;
  @Metric("# of getDelegationToken failed to be retrieved")
  private MutableGaugeInt numGetDelegationTokenFailedRetrieved;
  @Metric("# of renewDelegationToken failed to be retrieved")
  private MutableGaugeInt numRenewDelegationTokenFailedRetrieved;
  @Metric("# of renewDelegationToken failed to be retrieved")
  private MutableGaugeInt numCancelDelegationTokenFailedRetrieved;
  @Metric("# of dumpSchedulerLogs failed to be retrieved")
  private MutableGaugeInt numDumpSchedulerLogsFailedRetrieved;
  @Metric("# of getActivities failed to be retrieved")
  private MutableGaugeInt numGetActivitiesFailedRetrieved;
  @Metric("# of getBulkActivities failed to be retrieved")
  private MutableGaugeInt numGetBulkActivitiesFailedRetrieved;
  @Metric("# of getSchedulerInfo failed to be retrieved")
  private MutableGaugeInt numGetSchedulerInfoFailedRetrieved;
  @Metric("# of refreshSuperUserGroupsConfiguration failed to be retrieved")
  private MutableGaugeInt numRefreshSuperUserGroupsConfigurationFailedRetrieved;
  @Metric("# of refreshUserToGroupsMappings failed to be retrieved")
  private MutableGaugeInt numRefreshUserToGroupsMappingsFailedRetrieved;
  @Metric("# of deregisterSubCluster failed to be retrieved")
  private MutableGaugeInt numDeregisterSubClusterFailedRetrieved;
  @Metric("# of saveFederationQueuePolicy failed to be retrieved")
  private MutableGaugeInt numSaveFederationQueuePolicyFailedRetrieved;
  @Metric("# of batchSaveFederationQueuePolicies failed to be retrieved")
  private MutableGaugeInt numBatchSaveFederationQueuePoliciesFailedRetrieved;
  @Metric("# of listFederationQueuePolicies failed to be retrieved")
  private MutableGaugeInt numListFederationQueuePoliciesFailedRetrieved;
  @Metric("# of deleteFederationApplication failed to be retrieved")
  private MutableGaugeInt numDeleteFederationApplicationFailedRetrieved;
  @Metric("# of getFederationSubClusters failed to be retrieved")
  private MutableGaugeInt numGetFederationSubClustersFailedRetrieved;
  @Metric("# of deleteFederationPoliciesByQueues failed to be retrieved")
  private MutableGaugeInt numDeleteFederationPoliciesByQueuesRetrieved;
  @Metric("# of refreshAdminAcls failed to be retrieved")
  private MutableGaugeInt numRefreshAdminAclsFailedRetrieved;
  @Metric("# of refreshServiceAcls failed to be retrieved")
  private MutableGaugeInt numRefreshServiceAclsFailedRetrieved;
  @Metric("# of replaceLabelsOnNodes failed to be retrieved")
  private MutableGaugeInt numReplaceLabelsOnNodesFailedRetrieved;
  @Metric("# of replaceLabelsOnNode failed to be retrieved")
  private MutableGaugeInt numReplaceLabelsOnNodeFailedRetrieved;
  @Metric("# of addToClusterNodeLabels failed to be retrieved")
  private MutableGaugeInt numAddToClusterNodeLabelsFailedRetrieved;
  @Metric("# of removeFromClusterNodeLabels failed to be retrieved")
  private MutableGaugeInt numRemoveFromClusterNodeLabelsFailedRetrieved;
  @Metric("# of numUpdateSchedulerConfiguration failed to be retrieved")
  private MutableGaugeInt numUpdateSchedulerConfigurationFailedRetrieved;
  @Metric("# of numGetSchedulerConfiguration failed to be retrieved")
  private MutableGaugeInt numGetSchedulerConfigurationFailedRetrieved;
  @Metric("# of getClusterInfo failed to be retrieved")
  private MutableGaugeInt numGetClusterInfoFailedRetrieved;
  @Metric("# of getClusterUserInfo failed to be retrieved")
  private MutableGaugeInt numGetClusterUserInfoFailedRetrieved;
  @Metric("# of updateNodeResource failed to be retrieved")
  private MutableGaugeInt numUpdateNodeResourceFailedRetrieved;
  @Metric("# of refreshNodesResources failed to be retrieved")
  private MutableGaugeInt numRefreshNodesResourcesFailedRetrieved;
  @Metric("# of checkForDecommissioningNodes failed to be retrieved")
  private MutableGaugeInt numCheckForDecommissioningNodesFailedRetrieved;
  @Metric("# of refreshClusterMaxPriority failed to be retrieved")
  private MutableGaugeInt numRefreshClusterMaxPriorityFailedRetrieved;
  @Metric("# of mapAttributesToNodes failed to be retrieved")
  private MutableGaugeInt numMapAttributesToNodesFailedRetrieved;
  @Metric("# of getGroupsForUser failed to be retrieved")
  private MutableGaugeInt numGetGroupsForUserFailedRetrieved;

  // 成功操作聚合指标，统计成功次数和总耗时，无需每次调用查询
  @Metric("Total number of successful Submitted apps and latency(ms)")
  private MutableRate totalSucceededAppsSubmitted;
  @Metric("Total number of successful Killed apps and latency(ms)")
  private MutableRate totalSucceededAppsKilled;
  @Metric("Total number of successful Created apps and latency(ms)")
  private MutableRate totalSucceededAppsCreated;
  @Metric("Total number of successful Retrieved app reports and latency(ms)")
  private MutableRate totalSucceededAppsRetrieved;
  @Metric("Total number of successful Retrieved multiple apps reports and latency(ms)")
  private MutableRate totalSucceededMultipleAppsRetrieved;
  @Metric("Total number of successful Retrieved appAttempt reports and latency(ms)")
  private MutableRate totalSucceededAppAttemptsRetrieved;
  @Metric("Total number of successful Retrieved getClusterMetrics and latency(ms)")
  private MutableRate totalSucceededGetClusterMetricsRetrieved;
  @Metric("Total number of successful Retrieved getClusterNodes and latency(ms)")
  private MutableRate totalSucceededGetClusterNodesRetrieved;
  @Metric("Total number of successful Retrieved getNodeToLabels and latency(ms)")
  private MutableRate totalSucceededGetNodeToLabelsRetrieved;
  @Metric("Total number of successful Retrieved getNodeToLabels and latency(ms)")
  private MutableRate totalSucceededGetLabelsToNodesRetrieved;
  @Metric("Total number of successful Retrieved getClusterNodeLabels and latency(ms)")
  private MutableRate totalSucceededGetClusterNodeLabelsRetrieved;
  @Metric("Total number of successful Retrieved getApplicationAttemptReport and latency(ms)")
  private MutableRate totalSucceededAppAttemptReportRetrieved;
  @Metric("Total number of successful Retrieved getQueueUserAcls and latency(ms)")
  private MutableRate totalSucceededGetQueueUserAclsRetrieved;
  @Metric("Total number of successful Retrieved getContainerReport and latency(ms)")
  private MutableRate totalSucceededGetContainerReportRetrieved;
  @Metric("Total number of successful Retrieved getContainers and latency(ms)")
  private MutableRate totalSucceededGetContainersRetrieved;
  @Metric("Total number of successful Retrieved listReservations and latency(ms)")
  private MutableRate totalSucceededListReservationsRetrieved;
  @Metric("Total number of successful Retrieved getResourceTypeInfo and latency(ms)")
  private MutableRate totalSucceededGetResourceTypeInfoRetrieved;
  @Metric("Total number of successful Retrieved failApplicationAttempt and latency(ms)")
  private MutableRate totalSucceededFailAppAttemptRetrieved;
  @Metric("Total number of successful Retrieved updateApplicationPriority and latency(ms)")
  private MutableRate totalSucceededUpdateAppPriorityRetrieved;
  @Metric("Total number of successful Retrieved updateApplicationTimeouts and latency(ms)")
  private MutableRate totalSucceededUpdateAppTimeoutsRetrieved;
  @Metric("Total number of successful Retrieved signalToContainer and latency(ms)")
  private MutableRate totalSucceededSignalToContainerRetrieved;
  @Metric("Total number of successful Retrieved getQueueInfo and latency(ms)")
  private MutableRate totalSucceededGetQueueInfoRetrieved;
  @Metric("Total number of successful Retrieved moveApplicationAcrossQueues and latency(ms)")
  private MutableRate totalSucceededMoveApplicationAcrossQueuesRetrieved;
  @Metric("Total number of successful Retrieved getResourceProfiles and latency(ms)")
  private MutableRate totalSucceededGetResourceProfilesRetrieved;
  @Metric("Total number of successful Retrieved getResourceProfile and latency(ms)")
  private MutableRate totalSucceededGetResourceProfileRetrieved;
  @Metric("Total number of successful Retrieved getAttributesToNodes and latency(ms)")
  private MutableRate totalSucceededGetAttributesToNodesRetrieved;
  @Metric("Total number of successful Retrieved getClusterNodeAttributes and latency(ms)")
  private MutableRate totalSucceededGetClusterNodeAttributesRetrieved;
  @Metric("Total number of successful Retrieved getNodesToAttributes and latency(ms)")
  private MutableRate totalSucceededGetNodesToAttributesRetrieved;
  @Metric("Total number of successful Retrieved GetNewReservation and latency(ms)")
  private MutableRate totalSucceededGetNewReservationRetrieved;
  @Metric("Total number of successful Retrieved SubmitReservation and latency(ms)")
  private MutableRate totalSucceededSubmitReservationRetrieved;
  @Metric("Total number of successful Retrieved UpdateReservation and latency(ms)")
  private MutableRate totalSucceededUpdateReservationRetrieved;
  @Metric("Total number of successful Retrieved DeleteReservation and latency(ms)")
  private MutableRate totalSucceededDeleteReservationRetrieved;
  @Metric("Total number of successful Retrieved ListReservation and latency(ms)")
  private MutableRate totalSucceededListReservationRetrieved;
  @Metric("Total number of successful Retrieved GetAppActivities and latency(ms)")
  private MutableRate totalSucceededGetAppActivitiesRetrieved;
  @Metric("Total number of successful Retrieved GetAppStatistics and latency(ms)")
  private MutableRate totalSucceededGetAppStatisticsRetrieved;
  @Metric("Total number of successful Retrieved GetAppPriority and latency(ms)")
  private MutableRate totalSucceededGetAppPriorityRetrieved;
  @Metric("Total number of successful Retrieved GetAppQueue and latency(ms)")
  private MutableRate totalSucceededGetAppQueueRetrieved;
  @Metric("Total number of successful Retrieved UpdateAppQueue and latency(ms)")
  private MutableRate totalSucceededUpdateAppQueueRetrieved;
  @Metric("Total number of successful Retrieved GetAppTimeout and latency(ms)")
  private MutableRate totalSucceededGetAppTimeoutRetrieved;
  @Metric("Total number of successful Retrieved GetAppTimeouts and latency(ms)")
  private MutableRate totalSucceededGetAppTimeoutsRetrieved;
  @Metric("Total number of successful Retrieved RefreshQueues and latency(ms