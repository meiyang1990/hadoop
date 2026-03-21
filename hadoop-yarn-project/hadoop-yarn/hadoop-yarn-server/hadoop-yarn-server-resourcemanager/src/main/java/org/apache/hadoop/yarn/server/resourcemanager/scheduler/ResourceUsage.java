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

import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 按节点标签分类的资源使用情况统计，支持以下资源类型按标签统计：
 * - AM资源（YARN-2637之后用于按标签实现最大AM资源限制）
 * - 已使用资源（包含AM资源使用量）
 * - 预留资源
 * - 待分配资源
 * - 剩余可用资源
 * 
 * 本类可用于跟踪队列、用户、应用级别的资源使用情况，线程安全。
 */
public class ResourceUsage extends AbstractResourceUsage {
  // 无标签的缩写表示
  private static final String NL = CommonNodeLabelsManager.NO_LABEL;

  /**
   * 构造函数，初始化空资源使用统计。
   */
  public ResourceUsage() {
    super();
  }

  /*
   * 已使用资源相关方法
   */

  /**
   * 获取无标签分区的已使用资源总量。
   * @return 已使用资源
   */
  public Resource getUsed() {
    return getUsed(NL);
  }

  /**
   * 获取指定标签分区的已使用资源总量。
   * @param label 节点标签
   * @return 已使用资源
   */
  public Resource getUsed(String label) {
    return _get(label, ResourceType.USED);
  }

  /**
   * 增加指定标签分区的已使用资源量。
   * @param label 节点标签
   * @param res 待增加资源
   */
  public void incUsed(String label, Resource res) {
    _inc(label, ResourceType.USED, res);
  }

  /**
   * 增加无标签分区的已使用资源量。
   * @param res 待增加资源
   */
  public void incUsed(Resource res) {
    incUsed(NL, res);
  }

  /**
   * 减少无标签分区的已使用资源量。
   * @param res 待减少资源
   */
  public void decUsed(Resource res) {
    decUsed(NL, res);
  }

  /**
   * 减少指定标签分区的已使用资源量。
   * @param label 节点标签
   * @param res 待减少资源
   */
  public void decUsed(String label, Resource res) {
    _dec(label, ResourceType.USED, res);
  }

  /**
   * 设置无标签分区的已使用资源总量。
   * @param res 新的已使用资源总量
   */
  public void setUsed(Resource res) {
    setUsed(NL, res);
  }
  
  /**
   * 从另一个资源使用对象复制所有标签分区的已使用资源。
   * @param other 源资源使用对象
   */
  public void copyAllUsed(AbstractResourceUsage other) {
    writeLock.lock();
    try {
      for (Entry<String, UsageByLabel> entry : other.usages.entrySet()) {
        setUsed(entry.getKey(), Resources.clone(entry.getValue().getUsed()));
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 设置指定标签分区的已使用资源总量。
   * @param label 节点标签
   * @param res 新的已使用资源总量
   */
  public void setUsed(String label, Resource res) {
    _set(label, ResourceType.USED, res);
  }

  /*
   * 待分配资源相关方法
   */

  /**
   * 获取无标签分区的待分配资源总量。
   * @return 待分配资源
   */
  public Resource getPending() {
    return getPending(NL);
  }

  /**
   * 获取指定标签分区的待分配资源总量。
   * @param label 节点标签
   * @return 待分配资源
   */
  public Resource getPending(String label) {
    return _get(label, ResourceType.PENDING);
  }

  /**
   * 增加指定标签分区的待分配资源量。
   * @param label 节点标签
   * @param res 待增加资源
   */
  public void incPending(String label, Resource res) {
    _inc(label, ResourceType.PENDING, res);
  }

  /**
   * 增加无标签分区的待分配资源量。
   * @param res 待增加资源
   */
  public void incPending(Resource res) {
    incPending(NL, res);
  }

  /**
   * 减少无标签分区的待分配资源量。
   * @param res 待减少资源
   */
  public void decPending(Resource res) {
    decPending(NL, res);
  }

  /**
   * 减少指定标签分区的待分配资源量。
   * @param label 节点标签
   * @param res 待减少资源
   */
  public void decPending(String label, Resource res) {
    _dec(label, ResourceType.PENDING, res);
  }

  /**
   * 设置无标签分区的待分配资源总量。
   * @param res 新的待分配资源总量
   */
  public void setPending(Resource res) {
    setPending(NL, res);
  }

  /**
   * 设置指定标签分区的待分配资源总量。
   * @param label 节点标签
   * @param res 新的待分配资源总量
   */
  public void setPending(String label, Resource res) {
    _set(label, ResourceType.PENDING, res);
  }

  /*
   * 预留资源相关方法
   */

  /**
   * 获取无标签分区的预留资源总量。
   * @return 预留资源
   */
  public Resource getReserved() {
    return getReserved(NL);
  }

  /**
   * 获取指定标签分区的预留资源总量。
   * @param label 节点标签
   * @return 预留资源
   */
  public Resource getReserved(String label) {
    return _get(label, ResourceType.RESERVED);
  }

  /**
   * 增加指定标签分区的预留资源量。
   * @param label 节点标签
   * @param res 待增加资源
   */
  public void incReserved(String label, Resource res) {
    _inc(label, ResourceType.RESERVED, res);
  }

  /**
   * 增加无标签分区的预留资源量。
   * @param res 待增加资源
   */
  public void incReserved(Resource res) {
    incReserved(NL, res);
  }

  /**
   * 减少无标签分区的预留资源量。
   * @param res 待减少资源
   */
  public void decReserved(Resource res) {
    decReserved(NL, res);
  }

  /**
   * 减少指定标签分区的预留资源量。
   * @param label 节点标签
   * @param res 待减少资源
   */
  public void decReserved(String label, Resource res) {
    _dec(label, ResourceType.RESERVED, res);
  }

  /**
   * 设置无标签分区的预留资源总量。
   * @param res 新的预留资源总量
   */
  public void setReserved(Resource res) {
    setReserved(NL, res);
  }

  /**
   * 设置指定标签分区的预留资源总量。
   * @param label 节点标签
   * @param res 新的预留资源总量
   */
  public void setReserved(String label, Resource res) {
    _set(label, ResourceType.RESERVED, res);
  }

  /*
   * AM已使用资源相关方法
   */

  /**
   * 获取无标签分区的AM已使用资源总量。
   * @return AM已使用资源
   */
  public Resource getAMUsed() {
    return getAMUsed(NL);
  }

  /**
   * 获取指定标签分区的AM已使用资源总量。
   * @param label 节点标签
   * @return AM已使用资源
   */
  public Resource getAMUsed(String label) {
    return _get(label, ResourceType.AMUSED);
  }

  /**
   * 增加指定标签分区的AM已使用资源量。
   * @param label 节点标签
   * @param res 待增加资源
   */
  public void incAMUsed(String label, Resource res) {
    _inc(label, ResourceType.AMUSED, res);
  }

  /**
   * 增加无标签分区的AM已使用资源量。
   * @param res 待增加资源
   */
  public void incAMUsed(Resource res) {
    incAMUsed(NL, res);
  }

  /**
   * 减少无标签分区的AM已使用资源量。
   * @param res 待减少资源
   */
  public void decAMUsed(Resource res) {
    decAMUsed(NL, res);
  }

  /**
   * 减少指定标签分区的AM已使用资源量。
   * @param label 节点标签
   * @param res 待减少资源
   */
  public void decAMUsed(String label, Resource res) {
    _dec(label, ResourceType.AMUSED, res);
  }

  /**
   * 设置无标签分区的AM已使用资源总量。
   * @param res 新的AM已使用资源总量
   */
  public void setAMUsed(Resource res) {
    setAMUsed(NL, res);
  }

  /**
   * 设置指定标签分区的AM已使用资源总量。
   * @param label 节点标签
   * @param res 新的AM已使用资源总量
   */
  public void setAMUsed(String label, Resource res) {
    _set(label, ResourceType.AMUSED, res);
  }

  /**
   * 获取所有标签分区的待分配资源总和。
   * @return 所有标签分区待分配资源总和
   */
  public Resource getAllPending() {
    return _getAll(ResourceType.PENDING);
  }

  /**
   * 获取所有标签分区的已使用资源总和。
   * @return 所有标签分区已使用资源总和
   */
  public Resource getAllUsed() {
    return _getAll(ResourceType.USED);
  }

  /**
   * 获取所有标签分区的预留资源总和。
   * @return 所有标签分区预留资源总和
   */
  public Resource getAllReserved() {
    return _getAll(ResourceType.RESERVED);
  }

  /*
   * 缓存已使用资源相关方法
   */

  /**
   * 获取无标签分区的缓存已使用资源。
   * @return 缓存已使用资源
   */
  public Resource getCachedUsed() {
    return _get(NL, ResourceType.CACHED_USED);
  }

  /**
   * 获取指定标签分区的缓存已使用资源。
   * @param label 节点标签
   * @return 缓存已使用资源
   */
  public Resource getCachedUsed(String label) {
    return _get(label, ResourceType.CACHED_USED);
  }

  /**
   * 获取无标签分区的缓存待分配资源。
   * @return 缓存待分配资源
   */
  public Resource getCachedPending() {
    return _get(NL, ResourceType.CACHED_PENDING);
  }

  /**
   * 获取指定标签分区的缓存待分配资源。
   * @param label 节点标签
   * @return 缓存待分配资源
   */
  public Resource getCachedPending(String label) {
    return _get(label, ResourceType.CACHED_PENDING);
  }

  /**
   * 设置指定标签分区的缓存已使用资源。
   * @param label 节点标签
   * @param res 新的缓存已使用资源值
   */
  public void setCachedUsed(String label, Resource res) {
    _set(label, ResourceType.CACHED_USED, res);
  }

  /**
   * 设置无标签分区的缓存已使用资源。
   * @param res 新的缓存已使用资源值
   */
  public void setCachedUsed(Resource res) {
    _set(NL, ResourceType.CACHED_USED, res);
  }

  /**
   * 设置指定标签分区的缓存待分配资源。
   * @param label 节点标签
   * @param res 新的缓存待分配资源值
   */
  public void setCachedPending(String label, Resource res) {
    _set(label, ResourceType.CACHED_PENDING, res);
  }

  /**
   * 设置无标签分区的缓存待分配资源。
   * @param res 新的缓存待分配资源值
   */
  public void setCachedPending(Resource res) {
    _set(NL, ResourceType.CACHED_PENDING, res);
  }

  /*
   * AM资源限制相关方法
   */

  /**
   * 获取无标签分区的队列AM资源限制。
   * @return AM资源限制
   */
  public Resource getAMLimit() {
    return getAMLimit(NL);
  }

  /**
   * 获取指定标签分区的队列AM资源限制。
   * @param label 节点标签
   * @return AM资源限制
   */
  public Resource getAMLimit(String label) {
    return _get(label, ResourceType.AMLIMIT);
  }

  /**
   * 增加指定标签分区的队列AM资源限制。
   * @param label 节点标签
   * @param res 待增加资源量
   */
  public void incAMLimit(String label, Resource res) {
    _inc(label, ResourceType.AMLIMIT, res);
  }

  /**
   * 增加无标签分区的队列AM资源限制。
   * @param res 待增加资源量
   */
  public void incAMLimit(Resource res) {
    incAMLimit(NL, res);
  }

  /**
   * 减少无标签分区的队列AM资源限制。
   * @param res 待减少资源量
   */
  public void decAMLimit(Resource res) {
    decAMLimit(NL, res);
  }

  /**
   * 减少指定标签分区的队列AM资源限制。
   * @param label 节点标签
   * @param res 待减少资源量
   */
  public void decAMLimit(String label, Resource res) {
    _dec(label, ResourceType.AMLIMIT, res);
  }

  /**
   * 设置无标签分区的队列AM资源限制。
   * @param res 新的AM资源限制值
   */
  public void setAMLimit(Resource res) {
    setAMLimit(NL, res);
  }

  /**
   * 设置指定标签分区的队列AM资源限制。
   * @param label 节点标签
   * @param res 新的AM资源限制值
   */
  public void setAMLimit(String label, Resource res) {
    _set(label, ResourceType.AMLIMIT, res);
  }

  /**
   * 获取无标签分区的用户AM资源限制。
   * @return 用户AM资源限制
   */
  public Resource getUserAMLimit() {
    return getAMLimit(NL);
  }

  /**
   * 获取指定标签分区的用户AM资源限制。
   * @param label 节点标签
   * @return 用户AM资源限制
   */
  public Resource getUserAMLimit(String label) {
    return _get(label, ResourceType.USERAMLIMIT);
  }

  /**
   * 设置无标签分区的用户AM资源限制。
   * @param res 新的用户AM资源限制值
   */
  public void setUserAMLimit(Resource res) {
    setAMLimit(NL, res);
  }

  /**
   * 设置指定标签分区的用户AM资源限制。
   * @param label 节点标签
   * @param res 新的用户AM资源限制值
   */
  public void setUserAMLimit(String label, Resource res) {
    _set(label, ResourceType.USERAMLIMIT, res);
  }

  /**
   * 计算指定标签分区的总需求（缓存已使用 + 缓存待分配）。
   * @param label 节点标签
   * @return 总需求资源
   */
  public Resource getCachedDemand(String label) {
    readLock.lock();
    try {
      // 初始化总需求为0资源
      Resource demand = Resources.createResource(0);
      // 累加缓存已使用资源
      Resources.addTo(demand,