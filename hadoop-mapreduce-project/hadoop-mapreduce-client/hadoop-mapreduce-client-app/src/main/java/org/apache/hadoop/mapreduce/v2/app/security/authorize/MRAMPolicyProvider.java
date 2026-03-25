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
package org.apache.hadoop.mapreduce.v2.app.security.authorize;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;

/**
 * 文件：MRAMPolicyProvider.java
 * 所属模块：MapReduce客户端 -> 应用Master -> 安全授权
 * 核心职责：为MapReduce ApplicationMaster的RPC服务提供安全授权策略定义
 * 功能说明：注册MR AM需要进行权限校验的所有RPC服务，供Hadoop安全框架进行访问控制检查
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MRAMPolicyProvider extends PolicyProvider {
  
  // 预定义MR ApplicationMaster需要授权保护的所有RPC服务列表
  private static final Service[] mapReduceApplicationMasterServices = 
      new Service[] {
    new Service(
        MRJobConfig.MR_AM_SECURITY_SERVICE_AUTHORIZATION_TASK_UMBILICAL,
        TaskUmbilicalProtocol.class),
    new Service(
        MRJobConfig.MR_AM_SECURITY_SERVICE_AUTHORIZATION_CLIENT,
        MRClientProtocolPB.class)
  };

  /**
   * 获取当前MR AM需要授权保护的所有服务列表
   * @return 预定义的需要权限校验的服务数组
   */
  @Override
  public Service[] getServices() {
    return mapReduceApplicationMasterServices;
  }

}