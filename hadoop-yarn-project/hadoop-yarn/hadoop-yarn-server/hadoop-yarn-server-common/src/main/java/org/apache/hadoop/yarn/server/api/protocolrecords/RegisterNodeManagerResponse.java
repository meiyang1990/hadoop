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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

/**
 *  NodeManager向ResourceManager注册请求的响应抽象类，封装注册结果信息
 */
public abstract class RegisterNodeManagerResponse {
  /**
   * 获取容器令牌主密钥
   * @return 容器令牌主密钥
   */
  public abstract MasterKey getContainerTokenMasterKey();

  /**
   * 设置容器令牌主密钥
   * @param secretKey 容器令牌主密钥
   */
  public abstract void setContainerTokenMasterKey(MasterKey secretKey);

  /**
   * 获取NM令牌主密钥
   * @return NM令牌主密钥
   */
  public abstract MasterKey getNMTokenMasterKey();

  /**
   * 设置NM令牌主密钥
   * @param secretKey NM令牌主密钥
   */
  public abstract void setNMTokenMasterKey(MasterKey secretKey);

  /**
   * 获取ResourceManager要求NodeManager执行的节点动作
   * @return 节点动作
   */
  public abstract NodeAction getNodeAction();

  /**
   * 设置ResourceManager要求NodeManager执行的节点动作
   * @param nodeAction 节点动作
   */
  public abstract void setNodeAction(NodeAction nodeAction);

  /**
   * 获取ResourceManager实例标识，用于RM高可用切换识别
   * @return ResourceManager标识
   */
  public abstract long getRMIdentifier();

  /**
   * 设置ResourceManager实例标识
   * @param rmIdentifier ResourceManager标识
   */
  public abstract void setRMIdentifier(long rmIdentifier);

  /**
   * 获取注册诊断信息，注册失败时包含失败原因
   * @return 诊断信息字符串
   */
  public abstract String getDiagnosticsMessage();

  /**
   * 设置注册诊断信息
   * @param diagnosticsMessage 诊断信息字符串
   */
  public abstract void setDiagnosticsMessage(String diagnosticsMessage);

  /**
   * 设置ResourceManager版本信息
   * @param version ResourceManager版本
   */
  public abstract void setRMVersion(String version);

  /**
   * 获取ResourceManager版本信息
   * @return ResourceManager版本
   */
  public abstract String getRMVersion();

  /**
   * 获取ResourceManager认可的NodeManager总资源
   * @return 节点总资源
   */
  public abstract Resource getResource();

  /**
   * 设置ResourceManager认可的NodeManager总资源
   * @param resource 节点总资源
   */
  public abstract void setResource(Resource resource);

  /**
   * 获取节点标签是否被ResourceManager接受
   * @return 节点标签是否被接受
   */
  public abstract boolean getAreNodeLabelsAcceptedByRM();

  /**
   * 设置节点标签是否被ResourceManager接受
   * @param areNodeLabelsAcceptedByRM 节点标签是否被接受
   */
  public abstract void setAreNodeLabelsAcceptedByRM(
      boolean areNodeLabelsAcceptedByRM);

  /**
   * 获取节点属性是否被ResourceManager接受
   * @return 节点属性是否被接受
   */
  public abstract boolean getAreNodeAttributesAcceptedByRM();

  /**
   * 设置节点属性是否被ResourceManager接受
   * @param areNodeAttributesAcceptedByRM 节点属性是否被接受
   */
  public abstract void setAreNodeAttributesAcceptedByRM(
      boolean areNodeAttributesAcceptedByRM);
}