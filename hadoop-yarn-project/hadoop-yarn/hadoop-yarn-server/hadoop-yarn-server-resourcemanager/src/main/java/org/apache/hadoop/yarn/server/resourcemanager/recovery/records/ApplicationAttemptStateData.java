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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * 应用程序尝试（ApplicationAttempt）需要持久化存储的状态数据，用于RM恢复
 */
@Public
@Unstable
public abstract class ApplicationAttemptStateData {

  /**
   * 创建完整应用尝试状态数据实例，用于持久化恢复
   * @param attemptId 应用尝试ID
   * @param container AM容器
   * @param attemptTokens 应用尝试凭证
   * @param startTime 启动时间
   * @param finalState 最终状态
   * @param finalTrackingUrl 最终追踪URL
   * @param diagnostics 诊断信息
   * @param amUnregisteredFinalStatus 应用最终状态
   * @param exitStatus AM容器退出状态码
   * @param finishTime 完成时间
   * @param resourceSecondsMap 各资源累计使用秒数map
   * @param preemptedResourceSecondsMap 被抢占资源累计使用秒数map
   * @param totalAllocatedContainers 总共分配容器数
   * @return 应用尝试状态数据实例
   */
  public static ApplicationAttemptStateData newInstance(
      ApplicationAttemptId attemptId, Container container,
      Credentials attemptTokens, long startTime, RMAppAttemptState finalState,
      String finalTrackingUrl, String diagnostics,
      FinalApplicationStatus amUnregisteredFinalStatus, int exitStatus,
      long finishTime, Map<String, Long> resourceSecondsMap,
      Map<String, Long> preemptedResourceSecondsMap,
      int totalAllocatedContainers) {
    ApplicationAttemptStateData attemptStateData =
        Records.newRecord(ApplicationAttemptStateData.class);
    attemptStateData.setAttemptId(attemptId);
    attemptStateData.setMasterContainer(container);
    attemptStateData.setAppAttemptTokens(attemptTokens);
    attemptStateData.setState(finalState);
    attemptStateData.setFinalTrackingUrl(finalTrackingUrl);
    // 空诊断信息默认设置为空字符串
    attemptStateData.setDiagnostics(diagnostics == null ? "" : diagnostics);
    attemptStateData.setStartTime(startTime);
    attemptStateData.setFinalApplicationStatus(amUnregisteredFinalStatus);
    attemptStateData.setAMContainerExitStatus(exitStatus);
    attemptStateData.setFinishTime(finishTime);
    // 从资源map中获取内存MB秒数，不存在则默认0
    attemptStateData.setMemorySeconds(RMServerUtils
        .getOrDefault(resourceSecondsMap,
            ResourceInformation.MEMORY_MB.getName(), 0L));
    // 从资源map中获取vcore秒数，不存在则默认0
    attemptStateData.setVcoreSeconds(RMServerUtils
        .getOrDefault(resourceSecondsMap, ResourceInformation.VCORES.getName(),
            0L));
    // 从抢占资源map中获取抢占内存秒数，不存在则默认0
    attemptStateData.setPreemptedMemorySeconds(RMServerUtils
        .getOrDefault(preemptedResourceSecondsMap,
            ResourceInformation.MEMORY_MB.getName(), 0L));
    // 从抢占资源map中获取抢占vcore秒数，不存在则默认0
    attemptStateData.setPreemptedVcoreSeconds(RMServerUtils
        .getOrDefault(preemptedResourceSecondsMap,
            ResourceInformation.VCORES.getName(), 0L));
    attemptStateData.setResourceSecondsMap(resourceSecondsMap);
    attemptStateData
        .setPreemptedResourceSecondsMap(preemptedResourceSecondsMap);
    attemptStateData.setTotalAllocatedContainers(totalAllocatedContainers);
    return attemptStateData;
  }

  /**
   * 创建未完成运行的应用尝试状态数据实例
   * @param attemptId 应用尝试ID
   * @param masterContainer AM容器
   * @param attemptTokens 应用尝试凭证
   * @param startTime 启动时间
   * @param resourceSeondsMap 各资源累计使用秒数map
   * @param preemptedResourceSecondsMap 被抢占资源累计使用秒数map
   * @param totalAllocatedContainers 总共分配容器数
   * @return 应用尝试状态数据实例
   */
  public static ApplicationAttemptStateData newInstance(
      ApplicationAttemptId attemptId, Container masterContainer,
      Credentials attemptTokens, long startTime,
      Map<String, Long> resourceSeondsMap,
      Map<String, Long> preemptedResourceSecondsMap,
      int totalAllocatedContainers) {
    return newInstance(attemptId, masterContainer, attemptTokens, startTime,
        null, "N/A", "", null, ContainerExitStatus.INVALID, 0,
        resourceSeondsMap, preemptedResourceSecondsMap,
        totalAllocatedContainers);
  }


  public abstract ApplicationAttemptStateDataProto getProto();

  /**
   * 获取应用尝试ID
   * @return 应用尝试ID
   */
  @Public
  @Unstable
  public abstract ApplicationAttemptId getAttemptId();
  
  public abstract void setAttemptId(ApplicationAttemptId attemptId);
  
  /**
   * 获取运行该应用尝试的AM主容器
   * @return AM主容器
   */
  @Public
  @Unstable
  public abstract Container getMasterContainer();
  
  public abstract void setMasterContainer(Container container);

  /**
   * 获取该应用尝试对应的凭证信息
   * @return 应用尝试凭证
   */
  @Public
  @Unstable
  public abstract Credentials getAppAttemptTokens();

  public abstract void setAppAttemptTokens(Credentials attemptTokens);

  /**
   * 获取应用尝试的最终状态
   * @return 应用尝试最终状态
   */
  public abstract RMAppAttemptState getState();

  public abstract void setState(RMAppAttemptState state);

  /**
   * 获取应用尝试的最终未代理追踪URL，仅供代理本身使用
   * @return 最终未代理追踪URL
   */
  public abstract String getFinalTrackingUrl();

  /**
   * 设置AM的最终追踪URL
   * @param url 追踪URL
   */
  public abstract void setFinalTrackingUrl(String url);

  /**
   * 获取应用尝试的诊断信息
   * @return 诊断信息
   */
  public abstract String getDiagnostics();

  public abstract void setDiagnostics(String diagnostics);

  /**
   * 获取应用尝试的启动时间
   * @return 启动时间戳
   */
  public abstract long getStartTime();

  public abstract void setStartTime(long startTime);

  /**
   * 获取应用的最终完成状态
   * @return 应用最终完成状态
   */
  public abstract FinalApplicationStatus getFinalApplicationStatus();

  public abstract void setFinalApplicationStatus(
      FinalApplicationStatus finishState);

  public abstract int getAMContainerExitStatus();

  public abstract void setAMContainerExitStatus(int exitStatus);

  /**
   * 获取应用尝试的完成时间
   * @return 完成时间戳
   */
  public abstract long getFinishTime();

  public abstract void setFinishTime(long finishTime);

  /**
   * 获取应用累计内存使用量，单位MB*秒
   * @return 累计内存秒数
   */
  @Public
  @Unstable
  public abstract long getMemorySeconds();

  @Public
  @Unstable
  public abstract void setMemorySeconds(long memorySeconds);

  /**
   * 获取应用累计vcore使用量，单位核*秒
   * @return 累计vcore秒数
   */
  @Public
  @Unstable
  public abstract long getVcoreSeconds();

  @Public
  @Unstable
  public abstract void setVcoreSeconds(long vcoreSeconds);

  /**
   * 获取应用累计被抢占内存使用量，单位MB*秒
   * @return 累计被抢占内存秒数
   */
  @Public
  @Unstable
  public abstract long getPreemptedMemorySeconds();

  @Public
  @Unstable
  public abstract void setPreemptedMemorySeconds(long memorySeconds);

  /**
   * 获取应用累计被抢占vcore使用量，单位核*秒
   * @return 累计被抢占vcore秒数
   */
  @Public
  @Unstable
  public abstract long getPreemptedVcoreSeconds();

  @Public
  @Unstable
  public abstract void setPreemptedVcoreSeconds(long vcoreSeconds);

  /**
   * 获取各资源类型累计使用秒数map
   * @return key为资源名称，value为累计资源秒数
   */
  @Public
  @Unstable
  public abstract Map<String, Long> getResourceSecondsMap();

  /**
   * 设置各资源类型累计使用秒数map
   * @param resourceSecondsMap 各资源累计使用秒数map
   */
  @Public
  @Unstable
  public abstract void setResourceSecondsMap(
      Map<String, Long> resourceSecondsMap);

  /**
   * 获取各资源类型累计被抢占使用秒数map
   * @return key为资源名称，value为累计被抢占资源秒数
   */
  @Public
  @Unstable
  public abstract Map<String, Long> getPreemptedResourceSecondsMap();

  /**
   * 设置各资源类型累计被抢占使用秒数map
   * @param preemptedResourceSecondsMap 各资源累计被抢占使用秒数map
   */
  @Public
  @Unstable
  public abstract void setPreemptedResourceSecondsMap(
      Map<String, Long> preemptedResourceSecondsMap);

  /**
   * 获取该应用尝试总共分配的容器数
   * @return 总共分配容器数
   */
  @Public
  @Unstable
  public abstract int getTotalAllocatedContainers();

  /**
   * 设置该应用尝试总共分配的容器数
   * @param totalAllocatedContainers 总共分配容器数
   */
  @Public
  @Unstable
  public abstract void setTotalAllocatedContainers(
      int totalAllocatedContainers);

}