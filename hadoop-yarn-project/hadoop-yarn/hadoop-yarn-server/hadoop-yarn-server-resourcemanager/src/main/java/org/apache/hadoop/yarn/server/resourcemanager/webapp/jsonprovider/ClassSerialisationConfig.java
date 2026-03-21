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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerHealthInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ConfigVersionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ContainerLaunchContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LocalResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewReservation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.QueueAclInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.QueueAclsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteResponseInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationListInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateResponseInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInformationsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerOverviewInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UserMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UsersInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;
import org.apache.hadoop.yarn.webapp.dao.ConfInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * ResourceManager Web服务层JSON序列化配置类，管理需要特殊序列化处理的DTO类
 * 
 * <p>管理两类数据传输对象（DTO）的序列化行为：</p>
 * <ul>
 *   <li><b>带根包装类</b>：JSON输出包含根包装元素的类</li>
 *   <li><b>无跟包装类</b>：JSON输出省略根包装元素的类</li>
 * </ul>
 *
 * <p>配置初始化包含默认预定义类列表，同时支持从以下配置项加载用户自定义类：</p>
 * <ul>
 *   <li>{@code yarn.http.webapp.custom.dao.classes} - 自定义带根包装DTO类</li>
 *   <li>{@code yarn.http.webapp.custom.unwrapped.dao.classes} - 自定义无跟包装DTO类</li>
 * </ul>
 *
 * <p>该配置主要用于控制MOXy JSON提供器对REST API返回对象的序列化行为，确保输出格式符合API规范。</p>
 */
public class ClassSerialisationConfig {
  private static final Logger LOG = LoggerFactory.getLogger(ClassSerialisationConfig.class);

  // 默认自带的带根包装DTO类集合
  private static final Set<Class<?>> CONST_WRAPPED_CLASSES =
      Sets.newHashSet(ActivitiesInfo.class, AppActivitiesInfo.class, AppAttemptInfo.class,
          AppAttemptsInfo.class, AppInfo.class, ApplicationStatisticsInfo.class, AppsInfo.class,
          AppTimeoutInfo.class, AppTimeoutsInfo.class, BulkActivitiesInfo.class,
          CapacitySchedulerHealthInfo.class, CapacitySchedulerInfo.class,
          CapacitySchedulerQueueInfo.class, CapacitySchedulerQueueInfoList.class, ClusterInfo.class,
          ClusterMetricsInfo.class, ConfigVersionInfo.class, ContainerInfo.class,
          FairSchedulerQueueInfoList.class, FifoSchedulerInfo.class, NewReservation.class,
          NodeInfo.class, NodesInfo.class, QueueAclInfo.class, QueueAclsInfo.class,
          RemoteExceptionData.class, ReservationDeleteRequestInfo.class,
          ReservationDeleteResponseInfo.class, ReservationSubmissionRequestInfo.class,
          ReservationUpdateRequestInfo.class, ReservationUpdateResponseInfo.class,
          ResourceInfo.class, ResourceInformationsInfo.class, SchedulerInfo.class,
          SchedulerOverviewInfo.class, SchedulerTypeInfo.class, StatisticsItemInfo.class,
          UserInfo.class, UserMetricsInfo.class, UsersInfo.class);

  // 默认自带的无跟包装DTO类集合
  private static final Set<Class<?>> CONST_UNWRAPPED_CLASSES =
      Sets.newHashSet(ApplicationSubmissionContextInfo.class, AppPriority.class, AppQueue.class,
          AppState.class, ClusterUserInfo.class, ConfInfo.class, ContainersInfo.class ,
          ContainerLaunchContextInfo.class, DelegationToken.class, LabelsToNodesInfo.class,
          LocalResourceInfo.class, NewApplication.class, NodeLabelsInfo.class,
          NodeToLabelsEntryList.class, NodeToLabelsInfo.class, ReservationListInfo.class,
          ResourceOptionInfo.class, SchedConfUpdateInfo.class);

  private final Set<Class<?>> wrappedClasses;
  private final Set<Class<?>> unWrappedClasses;

  /**
   * 默认构造函数，使用空配置初始化
   */
  public ClassSerialisationConfig() {
    this(new Configuration());
  }

  /**
   * 构造序列化配置实例，从配置加载自定义DTO类完成初始化
   *
   * @param conf Hadoop配置对象，依赖注入获取，用于加载自定义DTO类配置
   */
  @Inject
  public ClassSerialisationConfig(@javax.inject.Named("conf") Configuration conf) {
    // 初始化带根包装类集合，加入默认类
    wrappedClasses = new HashSet<>(CONST_WRAPPED_CLASSES);
    try {
      // 从配置加载自定义带根包装DTO类并加入集合
      wrappedClasses.addAll(
          Arrays.asList(conf.getClasses(YarnConfiguration.YARN_HTTP_WEBAPP_CUSTOM_DAO_CLASSES)));
    } catch (RuntimeException e) {
      // 加载失败打警告日志，不影响启动
      LOG.warn("Failed to load YARN_HTTP_WEBAPP_CUSTOM_DAO_CLASSES", e);
    }

    // 初始化无跟包装类集合，加入默认类
    unWrappedClasses = new HashSet<>(CONST_UNWRAPPED_CLASSES);
    try {
      // 从配置加载自定义无跟包装DTO类并加入集合
      unWrappedClasses.addAll(Arrays.asList(
          conf.getClasses(YarnConfiguration.YARN_HTTP_WEBAPP_CUSTOM_UNWRAPPED_DAO_CLASSES)));
    } catch (RuntimeException e) {
      // 加载失败打警告日志，不影响启动
      LOG.warn("Failed to load YARN_HTTP_WEBAPP_CUSTOM_DAO_CLASSES", e);
    }

    // 追踪日志输出初始化完成的类集合
    LOG.trace("ClassSerialisationConfig was created, wrappedClasses: {} unWrappedClasses: {}",
        wrappedClasses, unWrappedClasses);

    // 检查是否有类同时出现在两个集合中，存在重复则抛出错误终止启动
    Set<Class<?>> duplicates = new HashSet<>(wrappedClasses);
    duplicates.retainAll(unWrappedClasses);
    if (!duplicates.isEmpty()) {
      throw new Error(String.format("Duplicate classes found: %s", duplicates));
    }
  }

  /**
   * 获取所有需要带根包装序列化的类集合
   * @return 带根包装类集合
   */
  public Set<Class<?>> getWrappedClasses() {
    return wrappedClasses;
  }

  /**
   * 获取所有需要无跟包装序列化的类集合
   * @return 无跟包装类集合
   */
  public Set<Class<?>> getUnWrappedClasses() {
    return unWrappedClasses;
  }
}