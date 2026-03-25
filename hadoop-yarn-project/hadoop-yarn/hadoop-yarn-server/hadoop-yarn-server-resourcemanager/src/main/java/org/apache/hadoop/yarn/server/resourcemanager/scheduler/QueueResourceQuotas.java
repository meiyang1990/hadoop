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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

/**
 * 按节点标签分类存储队列资源配额，跟踪队列/用户/应用的资源配额信息，包含配置最小资源和生效资源两种配额类型。
 * 该类线程安全，可在并发调度场景下安全访问。
 */
public class QueueResourceQuotas extends AbstractResourceUsage {
  // 无节点标签场景的缩写常量
  private static final String NL = CommonNodeLabelsManager.NO_LABEL;

  /**
   * 构造空队列资源配额对象。
   */
  public QueueResourceQuotas() {
    super();
  }

  /*
   * 配置的最小资源配额
   */

  /**
   * 获取无标签场景下配置的最小资源配额。
   * @return 配置的最小资源
   */
  public Resource getConfiguredMinResource() {
    return _get(NL, ResourceType.MIN_RESOURCE);
  }

  /**
   * 获取指定标签下配置的最小资源配额。
   * @param label 节点标签
   * @return 配置的最小资源
   */
  public Resource getConfiguredMinResource(String label) {
    return _get(label, ResourceType.MIN_RESOURCE);
  }

  /**
   * 设置指定标签下配置的最小资源配额。
   * @param label 节点标签
   * @param res 资源值
   */
  public void setConfiguredMinResource(String label, Resource res) {
    _set(label, ResourceType.MIN_RESOURCE, res);
  }

  /**
   * 设置无标签场景下配置的最小资源配额。
   * @param res 资源值
   */
  public void setConfiguredMinResource(Resource res) {
    _set(NL, ResourceType.MIN_RESOURCE, res);
  }

  /*
   * 配置的最大资源配额
   */

  /**
   * 获取无标签场景下配置的最大资源配额。
   * @return 配置的最大资源
   */
  public Resource getConfiguredMaxResource() {
    return getConfiguredMaxResource(NL);
  }

  /**
   * 获取指定标签下配置的最大资源配额。
   * @param label 节点标签
   * @return 配置的最大资源
   */
  public Resource getConfiguredMaxResource(String label) {
    return _get(label, ResourceType.MAX_RESOURCE);
  }

  /**
   * 设置无标签场景下配置的最大资源配额。
   * @param res 资源值
   */
  public void setConfiguredMaxResource(Resource res) {
    setConfiguredMaxResource(NL, res);
  }

  /**
   * 设置指定标签下配置的最大资源配额。
   * @param label 节点标签
   * @param res 资源值
   */
  public void setConfiguredMaxResource(String label, Resource res) {
    _set(label, ResourceType.MAX_RESOURCE, res);
  }

  /*
   * 生效的最小资源配额（调度计算后实际生效）
   */

  /**
   * 获取无标签场景下生效的最小资源配额。
   * @return 生效的最小资源
   */
  public Resource getEffectiveMinResource() {
    return _get(NL, ResourceType.EFF_MIN_RESOURCE);
  }

  /**
   * 获取指定标签下生效的最小资源配额。
   * @param label 节点标签
   * @return 生效的最小资源
   */
  public Resource getEffectiveMinResource(String label) {
    return _get(label, ResourceType.EFF_MIN_RESOURCE);
  }

  /**
   * 设置指定标签下生效的最小资源配额。
   * @param label 节点标签
   * @param res 资源值
   */
  public void setEffectiveMinResource(String label, Resource res) {
    _set(label, ResourceType.EFF_MIN_RESOURCE, res);
  }

  /**
   * 设置无标签场景下生效的最小资源配额。
   * @param res 资源值
   */
  public void setEffectiveMinResource(Resource res) {
    _set(NL, ResourceType.EFF_MIN_RESOURCE, res);
  }

  /*
   * 生效的最大资源配额（调度计算后实际生效）
   */

  /**
   * 获取无标签场景下生效的最大资源配额。
   * @return 生效的最大资源
   */
  public Resource getEffectiveMaxResource() {
    return getEffectiveMaxResource(NL);
  }

  /**
   * 获取指定标签下生效的最大资源配额。
   * @param label 节点标签
   * @return 生效的最大资源
   */
  public Resource getEffectiveMaxResource(String label) {
    return _get(label, ResourceType.EFF_MAX_RESOURCE);
  }

  /**
   * 设置无标签场景下生效的最大资源配额。
   * @param res 资源值
   */
  public void setEffectiveMaxResource(Resource res) {
    setEffectiveMax(NL, res);
  }

  /**
   * 设置指定标签下生效的最大资源配额。
   * @param label 节点标签
   * @param res 资源值
   */
  public void setEffectiveMaxResource(String label, Resource res) {
    _set(label, ResourceType.EFF_MAX_RESOURCE, res);
  }
}