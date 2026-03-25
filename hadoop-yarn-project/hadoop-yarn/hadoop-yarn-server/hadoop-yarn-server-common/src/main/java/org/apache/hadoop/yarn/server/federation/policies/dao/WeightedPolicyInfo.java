// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.dao;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN联邦权重策略配置数据访问对象，存储路由器和AMRM代理的子集群权重配置，以及负载均衡系数。
 * 权重的具体解释由对应策略实现决定，本类仅负责数据存储和序列化。
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
@XmlRootElement(name = "federation-policy")
@XmlAccessorType(XmlAccessType.FIELD)
public class WeightedPolicyInfo {

  private static final Logger LOG =
      LoggerFactory.getLogger(WeightedPolicyInfo.class);
  // Jackson JSON序列化/反序列化工具实例
  private static ObjectMapper mapper = new ObjectMapper();
  @JsonProperty("routerPolicyWeights")
  // 路由器路由策略的子集群权重映射
  private Map<SubClusterIdInfo, Float> routerPolicyWeights = new HashMap<>();
  @JsonProperty("amrmPolicyWeights")
  // AMRM代理策略的子集群权重映射
  private Map<SubClusterIdInfo, Float> amrmPolicyWeights = new HashMap<>();
  @JsonProperty("headroomAlpha")
  // 空闲资源权重系数，平衡静态权重和动态负载的影响
  private float headroomAlpha;

  public WeightedPolicyInfo() {
    // JAXB需要无参构造函数
  }

  /**
   * 从UTF-8编码的ByteBuffer反序列化生成WeightedPolicyInfo对象。
   *
   * @param bb 存储JSON格式配置的字节缓冲区
   *
   * @return 反序列化得到的WeightedPolicyInfo实例
   *
   * @throws FederationPolicyInitializationException 反序列化失败时抛出
   */
  public static WeightedPolicyInfo fromByteBuffer(ByteBuffer bb)
      throws FederationPolicyInitializationException {

    if (mapper == null) {
      throw new FederationPolicyInitializationException(
          "JSONJAXBContext should not be null.");
    }

    try {
      // 分配字节数组存储缓冲区数据
      final byte[] bytes = new byte[bb.remaining()];
      // 读取缓冲区内容到字节数组
      bb.get(bytes);
      // 转换为UTF-8编码的JSON字符串
      String params = new String(bytes, StandardCharsets.UTF_8);
      // Jackson反序列化得到对象
      return mapper.readValue(params, WeightedPolicyInfo.class);
    } catch (JsonProcessingException j) {
      throw new FederationPolicyInitializationException(j);
    }
  }

  /**
   * 获取路由器路由策略的子集群权重映射。
   *
   * @return 路由器策略权重映射
   */
  public Map<SubClusterIdInfo, Float> getRouterPolicyWeights() {
    return routerPolicyWeights;
  }

  /**
   * 设置路由器路由策略的子集群权重映射。
   *
   * @param policyWeights 路由器策略权重映射
   */
  public void setRouterPolicyWeights(
      Map<SubClusterIdInfo, Float> policyWeights) {
    this.routerPolicyWeights = policyWeights;
  }

  /**
   * 获取AMRM代理策略的子集群权重映射。
   *
   * @return AMRM代理策略权重映射
   */
  public Map<SubClusterIdInfo, Float> getAMRMPolicyWeights() {
    return amrmPolicyWeights;
  }

  /**
   * 设置AMRM代理策略的子集群权重映射。
   *
   * @param policyWeights AMRM代理策略权重映射
   */
  public void setAMRMPolicyWeights(Map<SubClusterIdInfo, Float> policyWeights) {
    this.amrmPolicyWeights = policyWeights;
  }

  /**
   * 将当前WeightedPolicyInfo序列化为UTF-8编码的ByteBuffer。
   *
   * @return 存储JSON格式配置的字节缓冲区
   *
   * @throws FederationPolicyInitializationException 序列化失败时抛出
   */
  public ByteBuffer toByteBuffer()
      throws FederationPolicyInitializationException {
    if (mapper == null) {
      throw new FederationPolicyInitializationException(
          "JSONJAXBContext should not be null.");
    }
    try {
      // 序列化为JSON字符串
      String value = mapper.writeValueAsString(this);
      // 转换为UTF-8字节数组并包装为ByteBuffer
      return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    } catch (JsonProcessingException j) {
      throw new FederationPolicyInitializationException(j);
    }
  }

  @Override
  public boolean equals(Object other) {
    // 空值和类型检查
    if (other == null || !other.getClass().equals(this.getClass())) {
      return false;
    }

    WeightedPolicyInfo otherPolicy = (WeightedPolicyInfo) other;
    Map<SubClusterIdInfo, Float> otherAMRMWeights =
        otherPolicy.getAMRMPolicyWeights();
    Map<SubClusterIdInfo, Float> otherRouterWeights =
        otherPolicy.getRouterPolicyWeights();

    // 比较AMRM权重集合是否相等
    boolean amrmWeightsMatch =
        otherAMRMWeights != null && getAMRMPolicyWeights() != null
            && CollectionUtils.isEqualCollection(otherAMRMWeights.entrySet(),
                getAMRMPolicyWeights().entrySet());

    // 比较路由器权重集合是否相等
    boolean routerWeightsMatch =
        otherRouterWeights != null && getRouterPolicyWeights() != null
            && CollectionUtils.isEqualCollection(otherRouterWeights.entrySet(),
                getRouterPolicyWeights().entrySet());

    // 两个权重集合都相等才返回true
    return amrmWeightsMatch && routerWeightsMatch;
  }

  @Override
  public int hashCode() {
    // 基于两个权重映射计算哈希值
    return 31 * amrmPolicyWeights.hashCode() + routerPolicyWeights.hashCode();
  }

  /**
   * 获取空闲资源权重系数headroomAlpha，该系数用于平衡静态权重和动态负载在路由决策中的占比。
   * 系数越接近1，决策越依赖当前子集群实际可用空闲资源；越接近0，决策越依赖静态权重，忽略当前负载。
   *
   * @return headroomAlpha系数值
   */
  public float getHeadroomAlpha() {
    return headroomAlpha;
  }

  /**
   * 设置空闲资源权重系数headroomAlpha，该系数用于平衡静态权重和动态负载在路由决策中的占比。
   * 系数越接近1，决策越依赖当前子集群实际可用空闲资源；越接近0，决策越依赖静态权重，忽略当前负载。
   *
   * @param headroomAlpha 平衡系数值
   */
  public void setHeadroomAlpha(float headroomAlpha) {
    this.headroomAlpha = headroomAlpha;
  }

  @Override
  public String toString() {
    try {
      // 序列化为JSON字符串返回
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return "Error serializing to string.";
    }
  }
}