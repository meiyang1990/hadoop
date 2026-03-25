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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

/**
 * YARN服务端各类对象构建工具类，提供统一的常用服务端对象构建方法。
 *
 */
public class YarnServerBuilderUtils {

  // 记录工厂实例，用于创建YARN API记录对象
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  /**
   * 构建仅包含节点动作和诊断信息的节点心跳响应。
   * @param action 节点需要执行的动作
   * @param diagnosticsMessage 诊断信息
   * @return 节点心跳响应对象
   */
  public static NodeHeartbeatResponse newNodeHeartbeatResponse(
      NodeAction action, String diagnosticsMessage) {
    NodeHeartbeatResponse response = recordFactory
        .newRecordInstance(NodeHeartbeatResponse.class);
    response.setNodeAction(action);
    response.setDiagnosticsMessage(diagnosticsMessage);
    return response;
  }

  /**
   * 构建完整的节点心跳响应，包含所有需要返回给NodeManager的信息。
   * @param responseId 响应ID
   * @param action 节点需要执行的动作
   * @param containersToCleanUp 需要清理的容器列表
   * @param applicationsToCleanUp 需要清理的应用列表
   * @param containerTokenMasterKey 容器令牌主密钥
   * @param nmTokenMasterKey NodeManager令牌主密钥
   * @param nextHeartbeatInterval 下一次心跳间隔
   * @return 完整的节点心跳响应对象
   */
  public static NodeHeartbeatResponse newNodeHeartbeatResponse(int responseId,
      NodeAction action, List<ContainerId> containersToCleanUp,
      List<ApplicationId> applicationsToCleanUp,
      MasterKey containerTokenMasterKey, MasterKey nmTokenMasterKey,
      long nextHeartbeatInterval) {
    NodeHeartbeatResponse response = recordFactory
        .newRecordInstance(NodeHeartbeatResponse.class);
    response.setResponseId(responseId);
    response.setNodeAction(action);
    response.setContainerTokenMasterKey(containerTokenMasterKey);
    response.setNMTokenMasterKey(nmTokenMasterKey);
    response.setNextHeartBeatInterval(nextHeartbeatInterval);
    if(containersToCleanUp != null) {
      response.addAllContainersToCleanup(containersToCleanUp);
    }
    if(applicationsToCleanUp != null) {
      response.addAllApplicationsToCleanup(applicationsToCleanUp);
    }
    return response;
  }

  /**
   * Build SystemCredentialsForAppsProto objects.
   *
   * @param applicationId Application ID
   * @param credentials HDFS Tokens
   * @return systemCredentialsForAppsProto
   */
  public static SystemCredentialsForAppsProto newSystemCredentialsForAppsProto(
      ApplicationId applicationId, ByteBuffer credentials) {
    SystemCredentialsForAppsProto systemCredentialsForAppsProto =
        SystemCredentialsForAppsProto.newBuilder()
            .setAppId(ProtoUtils.convertToProtoFormat(applicationId))
            .setCredentialsForApp(ProtoUtils.BYTE_STRING_INTERNER.intern(
                ProtoUtils.convertToProtoFormat(credentials.duplicate())))
            .build();
    return systemCredentialsForAppsProto;
  }

  /**
   * Convert Collection of SystemCredentialsForAppsProto proto objects to a Map
   * of ApplicationId to ByteBuffer.
   *
   * @param systemCredentials List of SystemCredentialsForAppsProto proto
   *          objects
   * @return systemCredentialsForApps Map of Application Id to ByteBuffer
   */
  public static Map<ApplicationId, ByteBuffer> convertFromProtoFormat(
      Collection<SystemCredentialsForAppsProto> systemCredentials) {

    Map<ApplicationId, ByteBuffer> systemCredentialsForApps =
        new HashMap<ApplicationId, ByteBuffer>(systemCredentials.size());
    for (SystemCredentialsForAppsProto proto : systemCredentials) {
      systemCredentialsForApps.put(
          ProtoUtils.convertFromProtoFormat(proto.getAppId()),
          ProtoUtils.convertFromProtoFormat(proto.getCredentialsForApp()));
    }
    return systemCredentialsForApps;
  }

  /**
   * Convert Map of Application Id to ByteBuffer to Collection of
   * SystemCredentialsForAppsProto proto objects.
   *
   * @param systemCredentialsForApps Map of Application Id to ByteBuffer
   * @return systemCredentials List of SystemCredentialsForAppsProto proto
   *         objects
   */
  public static List<SystemCredentialsForAppsProto> convertToProtoFormat(
      Map<ApplicationId, ByteBuffer> systemCredentialsForApps) {
    List<SystemCredentialsForAppsProto> systemCredentials =
        new ArrayList<SystemCredentialsForAppsProto>(
            systemCredentialsForApps.size());
    for (Map.Entry<ApplicationId, ByteBuffer> entry : systemCredentialsForApps
        .entrySet()) {
      SystemCredentialsForAppsProto proto =
          newSystemCredentialsForAppsProto(entry.getKey(), entry.getValue());
      systemCredentials.add(proto);
    }
    return systemCredentials;
  }
}