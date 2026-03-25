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
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN调度器中队列/应用的资源配额限制类，记录了可分配资源的最大总限制，不是额外配额。
 * 用于在调度时控制队列或应用能够使用的最大资源量，支持抢占预留资源等场景的特殊计算。
 */
public class ResourceLimits {
  // 资源使用总上限
  private volatile Resource limit;

  // 配合RESERVE_CONT_LOOK_ALL_NODES配置使用的特殊限制，表示为了分配新容器需要解除预留的资源量
  private volatile Resource amountNeededUnreserve;

  // 可用于下一次分配的剩余资源量，如果不足则需要考虑解除部分容器的预留
  private volatile Resource headroom;

  // 为高优先级阻塞队列预留的资源量
  private Resource blockedHeadroom;

  // 是否允许资源抢占
  private boolean allowPreempt = false;

  /**
   * 构造只包含总限制的资源配额对象
   * @param limit 资源总上限
   */
  public ResourceLimits(Resource limit) {
    this(limit, Resources.none());
  }

  /**
   * 构造完整的资源配额对象
   * @param limit 资源总上限
   * @param amountNeededUnreserve 需要解除预留的资源量
   */
  public ResourceLimits(Resource limit, Resource amountNeededUnreserve) {
    this.amountNeededUnreserve = amountNeededUnreserve;
    this.headroom = limit;
    this.limit = limit;
  }

  /**
   * 获取资源总上限
   * @return 资源总上限
   */
  public Resource getLimit() {
    return limit;
  }

  /**
   * 获取可用于下一次分配的剩余资源量
   * @return 剩余可用资源量
   */
  public Resource getHeadroom() {
    return headroom;
  }

  /**
   * 设置可用于下一次分配的剩余资源量
   * @param headroom 剩余可用资源量
   */
  public void setHeadroom(Resource headroom) {
    this.headroom = headroom;
  }

  /**
   * 获取需要解除预留的资源量
   * @return 需要解除预留的资源量
   */
  public Resource getAmountNeededUnreserve() {
    return amountNeededUnreserve;
  }

  /**
   * 设置资源总上限
   * @param limit 资源总上限
   */
  public void setLimit(Resource limit) {
    this.limit = limit;
  }

  /**
   * 设置需要解除预留的资源量
   * @param amountNeededUnreserve 需要解除预留的资源量
   */
  public void setAmountNeededUnreserve(Resource amountNeededUnreserve) {
    this.amountNeededUnreserve = amountNeededUnreserve;
  }

  /**
   * 获取是否允许抢占资源
   * @return 是否允许抢占
   */
  public boolean isAllowPreemption() {
    return allowPreempt;
  }

  /**
   * 设置是否允许抢占资源
   * @param allowPreempt 是否允许抢占
   */
  public void setIsAllowPreemption(boolean allowPreempt) {
   this.allowPreempt = allowPreempt;
  }

  /**
   * 增加为高优先级阻塞队列预留的资源量
   * @param resource 要预留的资源量
   */
  public void addBlockedHeadroom(Resource resource) {
    if (blockedHeadroom == null) {
      blockedHeadroom = Resource.newInstance(0, 0);
    }
    Resources.addTo(blockedHeadroom, resource);
  }

  /**
   * 获取为高优先级阻塞队列预留的总资源量
   * @return 预留资源总量
   */
  public Resource getBlockedHeadroom() {
    if (blockedHeadroom == null) {
      return Resources.none();
    }
    return blockedHeadroom;
  }

  /**
   * 获取扣除高优先级预留资源后的净可用资源上限
   * @return 净可用资源上限
   */
  public Resource getNetLimit() {
    if (blockedHeadroom != null) {
      return Resources.subtract(limit, blockedHeadroom);
    }
    return limit;
  }
}