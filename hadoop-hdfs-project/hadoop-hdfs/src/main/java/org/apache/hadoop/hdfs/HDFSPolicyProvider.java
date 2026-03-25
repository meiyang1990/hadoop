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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.ZKFCProtocol;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ReconfigurationProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeLifelineProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.GenericRefreshProtocol;

/**
 * HDFS协议的安全ACL策略提供者，向Hadoop安全授权框架注册所有HDFS RPC协议对应的访问控制配置
 * 为每个HDFS RPC协议绑定对应的ACL配置项，实现不同协议的独立访问权限控制
 */
@InterfaceAudience.Private
public class HDFSPolicyProvider extends PolicyProvider {
  // 存储所有HDFS RPC协议与对应ACL配置的映射关系
  private static final Service[] hdfsServices =
    new Service[] {
    new Service(CommonConfigurationKeys.SECURITY_CLIENT_PROTOCOL_ACL,
        ClientProtocol.class),
    new Service(CommonConfigurationKeys.SECURITY_CLIENT_DATANODE_PROTOCOL_ACL,
        ClientDatanodeProtocol.class),
    new Service(CommonConfigurationKeys.SECURITY_DATANODE_PROTOCOL_ACL,
        DatanodeProtocol.class),
    new Service(CommonConfigurationKeys.SECURITY_INTER_DATANODE_PROTOCOL_ACL, 
        InterDatanodeProtocol.class),
    new Service(CommonConfigurationKeys.SECURITY_NAMENODE_PROTOCOL_ACL,
        NamenodeProtocol.class),
    new Service(CommonConfigurationKeys.SECURITY_QJOURNAL_SERVICE_PROTOCOL_ACL,
        QJournalProtocol.class),
        new Service(
            CommonConfigurationKeys.SECURITY_INTERQJOURNAL_SERVICE_PROTOCOL_ACL,
            InterQJournalProtocol.class),
    new Service(CommonConfigurationKeys.SECURITY_HA_SERVICE_PROTOCOL_ACL,
        HAServiceProtocol.class),
    new Service(CommonConfigurationKeys.SECURITY_ZKFC_PROTOCOL_ACL,
        ZKFCProtocol.class),
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_POLICY, 
        RefreshAuthorizationPolicyProtocol.class),
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_USER_MAPPINGS, 
        RefreshUserMappingsProtocol.class),
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_GET_USER_MAPPINGS,
        GetUserMappingsProtocol.class),
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_CALLQUEUE,
        RefreshCallQueueProtocol.class),
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_GENERIC_REFRESH,
        GenericRefreshProtocol.class),
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_DATANODE_LIFELINE,
        DatanodeLifelineProtocol.class),
    new Service(
          CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_RECONFIGURATION,
          ReconfigurationProtocol.class)
  };
  
  /**
   * 获取所有已注册的HDFS服务协议与ACL配置映射
   * @return HDFS所有RPC服务的ACL策略数组
   */
  @Override
  public Service[] getServices() {
    return hdfsServices;
  }
}