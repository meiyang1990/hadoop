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

package org.apache.hadoop.yarn.server.resourcemanager.federation;

import java.io.StringWriter;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;

import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN联邦环境下，子集群ResourceManager向联邦状态存储发送的周期性心跳任务。
 * 用于上报子集群存活状态，并同步当前子集群的资源能力信息。
 */
public class FederationStateStoreHeartbeat implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreHeartbeat.class);

  // 当前子集群ID
  private final SubClusterId subClusterId;
  // 联邦状态存储服务客户端
  private final FederationStateStore stateStoreService;
  // 当前ResourceManager的资源调度器
  private final ResourceScheduler rs;
  // 序列化后的当前子集群资源能力(JSON格式)
  private String capability;
  // JAXB上下文解析器，用于JSON序列化
  private JAXBContextResolver resolver;

  /**
   * 构造联邦心跳任务实例。
   * @param subClusterId 当前子集群ID
   * @param stateStoreClient 联邦状态存储客户端
   * @param scheduler 当前ResourceManager的资源调度器
   * @param resolver JAXB上下文解析器
   */
  public FederationStateStoreHeartbeat(
      SubClusterId subClusterId,
      FederationStateStore stateStoreClient,
      ResourceScheduler scheduler,
      JAXBContextResolver resolver
  ) {
    this.stateStoreService = stateStoreClient;
    this.subClusterId = subClusterId;
    this.rs = scheduler;
    this.resolver = resolver;
    LOG.info("Initialized Federation membership for cluster with timestamp: {}. ",
        ResourceManager.getClusterTimeStamp());
  }

  /**
   * 获取当前子集群最新资源状态，序列化为JSON字符串。
   */
  private void updateClusterState() {
    try {
      // 基于当前调度器信息构造集群指标对象
      ClusterMetricsInfo clusterMetricsInfo = new ClusterMetricsInfo(rs);
      // 获取对应类型的JAXB上下文
      JAXBContext context = resolver.getContext(ClusterMetricsInfo.class);
      // 创建JSON序列化器
      Marshaller marshaller = context.createMarshaller();
      // 设置输出格式为JSON
      marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      // 序列化到字符串缓冲区
      StringWriter stringWriter = new StringWriter();
      marshaller.marshal(clusterMetricsInfo, stringWriter);
      // 保存序列化后的JSON结果
      capability = stringWriter.toString();
    } catch (Exception e) {
      LOG.warn("Exception while trying to generate cluster state,"
          + " so reverting to last know state.", e);
    }
  }

  @Override
  public synchronized void run() {
    try {
      // 更新最新的集群资源状态
      updateClusterState();
      // 构造心跳请求，包含子集群ID、运行状态、资源能力
      SubClusterHeartbeatRequest request = SubClusterHeartbeatRequest
          .newInstance(subClusterId, SubClusterState.SC_RUNNING, capability);
      // 向联邦状态存储发送心跳
      stateStoreService.subClusterHeartbeat(request);
      LOG.debug("Sending the heartbeat with capability: {}", capability);
    } catch (Exception e) {
      LOG.warn("Exception when trying to heartbeat.", e);
    }
  }
}