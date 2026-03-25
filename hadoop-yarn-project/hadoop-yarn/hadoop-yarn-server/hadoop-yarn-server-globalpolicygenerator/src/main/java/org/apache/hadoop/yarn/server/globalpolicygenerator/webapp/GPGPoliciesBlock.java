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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import com.google.inject.Inject;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * 全局策略生成器(GPG)Web UI的策略概览HTML块，展示联邦集群策略配置信息
 */
public class GPGPoliciesBlock extends HtmlBlock {

  // 全局策略生成器实例
  private final GlobalPolicyGenerator gpg;

  // 联邦状态存储门面，用于获取策略配置
  private final FederationStateStoreFacade facade;

  @Inject
  GPGPoliciesBlock(GlobalPolicyGenerator gpg, ViewContext ctx) {
    super(ctx);
    this.gpg = gpg;
    // 从全局策略生成器配置初始化联邦状态存储门面
    this.facade = FederationStateStoreFacade.getInstance(gpg.getConfig());
  }

  @Override
  protected void render(Block html) {
    try {
      // 获取所有队列的策略配置集合
      Collection<SubClusterPolicyConfiguration> policies =
          facade.getPoliciesConfigurations().values();
      // 渲染YARN联邦策略表格
      initYarnFederationPolicies(policies, html);
    } catch (Exception e) {
      LOG.error("Get GPGPolicies Error.", e);
    }
  }

  /**
   * 初始化并渲染YARN联邦策略配置表格
   * @param policies 策略配置集合
   * @param html HTML块上下文
   * @throws FederationPolicyInitializationException 策略反序列化异常
   */
  private void initYarnFederationPolicies(Collection<SubClusterPolicyConfiguration> policies,
      Block html) throws FederationPolicyInitializationException {

    // 创建策略表格表头，定义各列含义
    Hamlet.TBODY<Hamlet.TABLE<Hamlet>> tbody = html.table("#policies").
        thead().
        tr().
        th(".queue", "Queue Name").
        th(".policyType", "Policy Type").
        th(".routerPolicyWeights", "Router PolicyWeights").
        th(".amrmPolicyWeights", "Router AMRMPolicyWeights").
        th(".headroomAlpha", "Router Headroom Alpha").
        __().__().
        tbody();

    if (policies != null) {
      // 遍历所有策略配置，生成表格行
      for (SubClusterPolicyConfiguration policy : policies) {
        // 创建新行，填充队列名称
        Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet>>> row = tbody.tr().td(policy.getQueue());
        // 填充策略类型
        String type = policy.getType();
        row = row.td(type);

        // 反序列化权重策略信息
        ByteBuffer params = policy.getParams();
        WeightedPolicyInfo weightedPolicyInfo = WeightedPolicyInfo.fromByteBuffer(params);
        // 填充路由策略权重
        row = row.td(policyWeight2String(weightedPolicyInfo.getRouterPolicyWeights()));
        // 填充AMRM策略权重
        row = row.td(policyWeight2String(weightedPolicyInfo.getAMRMPolicyWeights()));
        // 填充空闲资源Alpha参数，结束当前行
        row.td(String.valueOf(weightedPolicyInfo.getHeadroomAlpha())).__();
      }
    }

    // 结束表格渲染
    tbody.__().__();
  }

  /**
   * 将子集群权重映射转换为可展示的字符串格式
   *
   * @param weights 子集群ID到权重的映射
   * @return 格式化字符串 格式: SC-1:0.91, SC-2:0.09
   */
  private String policyWeight2String(Map<SubClusterIdInfo, Float> weights) {
    StringBuilder sb = new StringBuilder();
    // 拼接每个子集群的权重信息
    for (Map.Entry<SubClusterIdInfo, Float> entry : weights.entrySet()) {
      sb.append(entry.getKey().toId()).append(": ").append(entry.getValue()).append(", ");
    }
    // 移除末尾多余的逗号和空格
    if (sb.length() > 2) {
      sb.setLength(sb.length() - 2);
    }
    return sb.toString();
  }
}