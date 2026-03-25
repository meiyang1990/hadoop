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

package org.apache.hadoop.yarn.server.utils;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.api.ContainerType;

/**
 * YARN服务端各类YARN API对象的构造工具类，提供各类记录对象的便捷创建方法
 *
 */
@Private
public class BuilderUtils {

  /** 全局记录工厂实例，用于创建各类YARN API记录对象 */
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  /** ApplicationId比较器，用于对应用ID进行排序 */
  public static class ApplicationIdComparator implements
      Comparator<ApplicationId>, Serializable {
    @Override
    public int compare(ApplicationId a1, ApplicationId a2) {
      return a1.compareTo(a2);
    }
  }

  /** ContainerId比较器，用于对容器ID进行排序 */
  public static class ContainerIdComparator implements
      java.util.Comparator<ContainerId>, Serializable {

    @Override
    public int compare(ContainerId c1,
        ContainerId c2) {
      return c1.compareTo(c2);
    }
  }

  /** 创建LocalResource实例，设置所有必要属性 */
  public static LocalResource newLocalResource(URL url, LocalResourceType type,
      LocalResourceVisibility visibility, long size, long timestamp,
      boolean shouldBeUploadedToSharedCache) {
    LocalResource resource =
      recordFactory.newRecordInstance(LocalResource.class);
    resource.setResource(url);
    resource.setType(type);
    resource.setVisibility(visibility);
    resource.setSize(size);
    resource.setTimestamp(timestamp);
    resource.setShouldBeUploadedToSharedCache(shouldBeUploadedToSharedCache);
    return resource;
  }

  /** 基于URI创建LocalResource实例 */
  public static LocalResource newLocalResource(URI uri,
      LocalResourceType type, LocalResourceVisibility visibility, long size,
      long timestamp, boolean shouldBeUploadedToSharedCache) {
    return newLocalResource(URL.fromURI(uri), type,
        visibility, size, timestamp, shouldBeUploadedToSharedCache);
  }

  /**
   * 基于字符串ID创建ApplicationId实例
   */
  public static ApplicationId newApplicationId(RecordFactory recordFactory,
      long clustertimestamp, CharSequence id) {
    return ApplicationId.newInstance(clustertimestamp,
        Integer.parseInt(id.toString()));
  }

  /**
   * 基于整数ID创建ApplicationId实例，使用指定记录工厂
   */
  public static ApplicationId newApplicationId(RecordFactory recordFactory,
      long clusterTimeStamp, int id) {
    return ApplicationId.newInstance(clusterTimeStamp, id);
  }

  /**
   * 基于整数ID创建ApplicationId实例，使用全局记录工厂
   */
  public static ApplicationId newApplicationId(long clusterTimeStamp, int id) {
    return ApplicationId.newInstance(clusterTimeStamp, id);
  }

  /**
   * 创建ApplicationAttemptId实例
   */
  public static ApplicationAttemptId newApplicationAttemptId(
      ApplicationId appId, int attemptId) {
    return ApplicationAttemptId.newInstance(appId, attemptId);
  }

  /**
   * 将时间戳和字符串ID转换为ApplicationId
   */
  public static ApplicationId convert(long clustertimestamp, CharSequence id) {
    return ApplicationId.newInstance(clustertimestamp,
        Integer.parseInt(id.toString()));
  }

  /**
   * 创建ContainerId实例
   */
  public static ContainerId newContainerId(ApplicationAttemptId appAttemptId,
      long containerId) {
    return ContainerId.newContainerId(appAttemptId, containerId);
  }

  /**
   * 创建完整层级的ContainerId实例，从应用ID开始构造
   */
  public static ContainerId newContainerId(int appId, int appAttemptId,
      long timestamp, long id) {
    ApplicationId applicationId = newApplicationId(timestamp, appId);
    ApplicationAttemptId applicationAttemptId = newApplicationAttemptId(
        applicationId, appAttemptId);
    ContainerId cId = newContainerId(applicationAttemptId, id);
    return cId;
  }

  /**
   * 创建容器令牌实例，包含完整标识信息
   */
  public static Token newContainerToken(ContainerId cId, int containerVersion,
      String host, int port, String user, Resource r, long expiryTime,
      int masterKeyId, byte[] password, long rmIdentifier) throws IOException {
    ContainerTokenIdentifier identifier =
        new ContainerTokenIdentifier(cId, containerVersion, host + ":" + port,
            user, r, expiryTime, masterKeyId, rmIdentifier,
            Priority.newInstance(0), 0, null, CommonNodeLabelsManager.NO_LABEL,
            ContainerType.TASK, ExecutionType.GUARANTEED);
    return newContainerToken(BuilderUtils.newNodeId(host, port), password,
        identifier);
  }

  /**
   * 创建ContainerId实例，使用指定记录工厂
   */
  public static ContainerId newContainerId(RecordFactory recordFactory,
      ApplicationId appId, ApplicationAttemptId appAttemptId,
      int containerId) {
    return ContainerId.newContainerId(appAttemptId, containerId);
  }

  /**
   * 创建NodeId实例
   */
  public static NodeId newNodeId(String host, int port) {
    return NodeId.newInstance(host, port);
  }

  /**
   * 创建基础NodeReport实例，不包含资源利用率等扩展信息
   */
  public static NodeReport newNodeReport(NodeId nodeId, NodeState nodeState,
      String httpAddress, String rackName, Resource used, Resource capability,
      int numContainers, String healthReport, long lastHealthReportTime) {
    return newNodeReport(nodeId, nodeState, httpAddress, rackName, used,
        capability, numContainers, healthReport, lastHealthReportTime,
        null, null, null);
  }

  /**
   * 创建带节点标签和退役超时的NodeReport实例
   */
  public static NodeReport newNodeReport(NodeId nodeId, NodeState nodeState,
      String httpAddress, String rackName, Resource used, Resource capability,
      int numContainers, String healthReport, long lastHealthReportTime,
      Set<String> nodeLabels, Integer decommissioningTimeout,
      NodeUpdateType nodeUpdateType) {
    return newNodeReport(nodeId, nodeState, httpAddress, rackName, used,
        capability, numContainers, healthReport, lastHealthReportTime,
        nodeLabels, null, null, decommissioningTimeout, nodeUpdateType, null);
  }

  /** 创建完整属性的NodeReport实例 */
  public static NodeReport newNodeReport(NodeId nodeId, NodeState nodeState,
      String httpAddress, String rackName, Resource used, Resource capability,
      int numContainers, String healthReport, long lastHealthReportTime,
      Set<String> nodeLabels, ResourceUtilization containersUtilization,
      ResourceUtilization nodeUtilization, Integer decommissioningTimeout,
      NodeUpdateType nodeUpdateType, Set<NodeAttribute> attrs) {
    NodeReport nodeReport = recordFactory.newRecordInstance(NodeReport.class);
    nodeReport.setNodeId(nodeId);
    nodeReport.setNodeState(nodeState);
    nodeReport.setHttpAddress(httpAddress);
    nodeReport.setRackName(rackName);
    nodeReport.setUsed(used);
    nodeReport.setCapability(capability);
    nodeReport.setNumContainers(numContainers);
    nodeReport.setHealthReport(healthReport);
    nodeReport.setLastHealthReportTime(lastHealthReportTime);
    nodeReport.setNodeLabels(nodeLabels);
    nodeReport.setAggregatedContainersUtilization(containersUtilization);
    nodeReport.setNodeUtilization(nodeUtilization);
    nodeReport.setDecommissioningTimeout(decommissioningTimeout);
    nodeReport.setNodeUpdateType(nodeUpdateType);
    nodeReport.setNodeAttributes(attrs);
    return nodeReport;
  }

  /** 创建ContainerStatus实例，默认使用GURANTEED执行类型 */
  public static ContainerStatus newContainerStatus(ContainerId containerId,
      ContainerState containerState, String diagnostics, int exitStatus,
      Resource capability) {
    return newContainerStatus(containerId, containerState, diagnostics,
        exitStatus, capability, ExecutionType.GUARANTEED);
  }

  /** 创建完整属性的ContainerStatus实例 */
  public static ContainerStatus newContainerStatus(ContainerId containerId,
      ContainerState containerState, String diagnostics, int exitStatus,
      Resource capability, ExecutionType executionType) {
    ContainerStatus containerStatus = recordFactory
      .newRecordInstance(ContainerStatus.class);
    containerStatus.setState(containerState);
    containerStatus.setContainerId(containerId);
    containerStatus.setDiagnostics(diagnostics);
    containerStatus.setExitStatus(exitStatus);
    containerStatus.setCapability(capability);
    containerStatus.setExecutionType(executionType);
    return containerStatus;
  }

  /** 创建完整属性的Container实例 */
  public static Container newContainer(ContainerId containerId, NodeId nodeId,
      String nodeHttpAddress, Resource resource, Priority priority,
      Token containerToken, ExecutionType executionType,
      long allocationRequestId) {
    Container container = recordFactory.newRecordInstance(Container.class);
    container.setId(containerId);
    container.setNodeId(nodeId);
    container.setNodeHttpAddress(nodeHttpAddress);
    container.setResource(resource);
    container.setPriority(priority);
    container.setContainerToken(containerToken);
    container.setExecutionType(executionType);
    container.setAllocationRequestId(allocationRequestId);
    return container;
  }

  /** 创建Container实例，使用默认执行类型和分配请求ID */
  public static Container newContainer(ContainerId containerId, NodeId nodeId,
      String nodeHttpAddress, Resource resource, Priority priority,
      Token containerToken) {
    return newContainer(containerId, nodeId, nodeHttpAddress, resource,
        priority, containerToken, ExecutionType.GUARANTEED, 0);
  }

  /** 创建Container实例，使用默认执行类型 */
  public static Container newContainer(ContainerId containerId, NodeId nodeId,
      String nodeHttpAddress, Resource resource, Priority priority,
      Token containerToken, long allocationRequestId) {
    return newContainer(containerId, nodeId, nodeHttpAddress, resource,
        priority, containerToken, ExecutionType.GUARANTEED,
        allocationRequestId);
  }

  /** 创建通用令牌实例 */
  public static <T extends Token> T newToken(Class<T> tokenClass,
      byte[] identifier, String kind, byte[] password, String service) {
    T token = recordFactory.newRecordInstance(tokenClass);
    token.setIdentifier(ByteBuffer.wrap(identifier));
    token.setKind(kind);
    token.setPassword(ByteBuffer.wrap(password));
    token.setService(service);
    return token;
  }

  /** 创建委托令牌实例 */
  public static Token newDelegationToken(byte[] identifier,
      String kind, byte[] password, String service) {
    return newToken(Token.class, identifier, kind, password, service);
  }

  /** 创建客户端到AM的令牌实例 */
  public static Token newClientToAMToken(byte[] identifier, String kind,
      byte[] password, String service) {
    return newToken(Token.class, identifier, kind, password, service);
  }

  /** 创建AM到RM的令牌实例 */
  public static Token newAMRMToken(byte[] identifier, String kind,
                                   byte[] password, String service) {
    return newToken(Token.class, identifier, kind, password, service);
  }

  /**
   * 基于容器令牌标识创建容器令牌，仅用于测试
   */
  @VisibleForTesting
  public static Token newContainerToken(NodeId nodeId,
      byte[] password, ContainerTokenIdentifier tokenIdentifier) {
    // RPC层客户端要求令牌服务使用ip:port格式
    InetSocketAddress addr =
        NetUtils.createSocketAddrForHost(nodeId.getHost(), nodeId.getPort());
    // 注意：如果要生产使用令牌，应该使用SecurityUtil.setTokenService
    Token containerToken =
        newToken(Token.class, tokenIdentifier.getBytes(),
          ContainerTokenIdentifier.KIND.toString(), password, SecurityUtil
            .buildTokenService(addr).toString());
    return containerToken;
  }

  /**
   * 从YARN令牌反序列化得到ContainerTokenIdentifier
   */
  public static ContainerTokenIdentifier newContainerTokenIdentifier(
      Token containerToken) throws IOException {
    org.apache.hadoop.security.token.Token<ContainerTokenIdentifier> token =
        new org.apache.hadoop.security.token.Token<ContainerTokenIdentifier>(
            containerToken.getIdentifier()
                .array(), containerToken.getPassword().array(), new Text(
                containerToken.getKind()),
            new Text(containerToken.getService()));
    return token.decodeIdentifier();
  }

  /** 创建ContainerLaunchContext实例，设置所有核心属性 */
  public static ContainerLaunchContext newContainerLaunchContext(
      Map<String, LocalResource> localResources,
      Map<String, String> environment, List<String> commands,
      Map<String, ByteBuffer> serviceData, ByteBuffer tokens,
      Map<ApplicationAccessType, String> acls) {
    ContainerLaunchContext container = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    container.setLocalResources(localResources);
    container.setEnvironment(environment);
    container.setCommands(commands);
    container.setServiceData(serviceData);
    container.setTokens(tokens);
    container.setApplicationACLs(acls);
    return container;
  }

  /** 创建指定优先级的Priority实例 */
  public static Priority newPriority(int p) {
    Priority priority = recordFactory.newRecordInstance(Priority.class);
    priority.setPriority(p);
    return priority;
  }

  /** 创建ResourceRequest实例，不带节点标签 */
  public static ResourceRequest newResourceRequest(Priority priority,
      String hostName, Resource capability, int numContainers) {
    ResourceRequest request = recordFactory
        .newRecordInstance(ResourceRequest.class);
    request.setPriority(priority);
    request.setResourceName(hostName);
    request.setCapability(capability);
    request.setNumContainers(numContainers);
    request.setExecutionTypeRequest(ExecutionTypeRequest.newInstance());
    return