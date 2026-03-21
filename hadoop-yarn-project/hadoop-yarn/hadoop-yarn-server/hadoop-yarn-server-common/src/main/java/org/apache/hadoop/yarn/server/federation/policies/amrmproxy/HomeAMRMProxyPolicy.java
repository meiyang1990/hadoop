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

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * {@link FederationAMRMProxyPolicy}的实现类，将所有{@link ResourceRequest}
 * 全部路由到应用所属的本地home子集群，是联邦场景下AM-RM代理的简单路由策略。
 */
public class HomeAMRMProxyPolicy extends AbstractAMRMProxyPolicy {

  /** Home子集群的标识符。 */
  private SubClusterId homeSubcluster;

  @Override
  public void reinitialize(
      FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {
    // 验证策略初始化上下文合法性
    FederationPolicyInitializationContextValidator
        .validate(policyContext, this.getClass().getCanonicalName());
    // 保存策略上下文到父类
    setPolicyContext(policyContext);

    // 从上下文中获取当前应用所属的home子集群ID
    this.homeSubcluster = policyContext.getHomeSubcluster();
  }

  @Override
  public Map<SubClusterId, List<ResourceRequest>> splitResourceRequests(
      List<ResourceRequest> resourceRequests,
      Set<SubClusterId> timedOutSubClusters) throws YarnException {
    // home子集群未初始化，抛出异常
    if (homeSubcluster == null) {
      throw new FederationPolicyException("No home subcluster available");
    }

    // 获取当前所有活跃子集群信息
    Map<SubClusterId, SubClusterInfo> active = getActiveSubclusters();
    // home子集群不在活跃列表中，抛出异常
    if (!active.containsKey(homeSubcluster)) {
      throw new FederationPolicyException(
          "The local subcluster " + homeSubcluster + " is not active");
    }

    // 复制全部资源请求
    List<ResourceRequest> resourceRequestsCopy =
        new ArrayList<>(resourceRequests);
    // 将所有请求全部打包返回给home子集群
    return Collections.singletonMap(homeSubcluster, resourceRequestsCopy);
  }
}