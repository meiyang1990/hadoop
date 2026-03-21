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

package org.apache.hadoop.yarn.server.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 按相同调度键{@link ResourceRequestSetKey}分组的资源请求集合，
 * 用于聚合同一规格、同一执行类型的所有位置相关资源请求。
 */
public class ResourceRequestSet {

  private ResourceRequestSetKey key;
  private int numContainers;
  // Whether the ANY RR is relaxable
  private boolean relaxable;
  // ResourceName -> RR
  private Map<String, ResourceRequest> asks;

  /**
   * 创建一个空的资源请求集合，使用指定的调度键。
   *
   * @param key 资源请求集合的调度键
   * @throws YarnException 创建失败时抛出异常
   */
  public ResourceRequestSet(ResourceRequestSetKey key) throws YarnException {
    this.key = key;
    // leave it zero for now, as if it is a cancel
    this.numContainers = 0;
    this.relaxable = true;
    this.asks = new HashMap<>();
  }

  /**
   * 浅拷贝构造方法，复制另一个资源请求集合。
   *
   * @param other 要复制的源资源请求集合
   */
  public ResourceRequestSet(ResourceRequestSet other) {
    this.key = other.key;
    this.numContainers = other.numContainers;
    this.asks = new HashMap<>();
    this.relaxable = other.relaxable;
    // The assumption is that the RR objects should not be modified without
    // making a copy
    this.asks.putAll(other.asks);
  }

  /**
   * 添加资源请求，如果对应位置已存在请求则覆盖，同时更新集合统计信息。
   *
   * @param ask 要添加的新资源请求
   * @throws YarnException 请求不匹配或容器数非法时抛出异常
   */
  public void addAndOverrideRR(ResourceRequest ask) throws YarnException {
    // 检查请求是否匹配当前集合的调度键
    if (!this.key.equals(new ResourceRequestSetKey(ask))) {
      throw new YarnException(
          "None compatible asks: \n" + ask + "\n" + this.key);
    }

    // Override directly if exists
    this.asks.put(ask.getResourceName(), ask);

    // 保证型执行类型，仅更新ANY请求的容器数和位置松弛配置
    if (this.key.getExeType().equals(ExecutionType.GUARANTEED)) {
      // For G requestSet, update the numContainers only for ANY RR
      if (ask.getResourceName().equals(ResourceRequest.ANY)) {
        this.numContainers = ask.getNumContainers();
        this.relaxable = ask.getRelaxLocality();
      }
    } else {
      // The assumption we made about O asks is that all RR in a requestSet has
      // the same numContainers value. So we just take the value of the last RR
      // 机会型执行类型，所有请求容器数一致，直接使用最后一个请求的值
      this.numContainers = ask.getNumContainers();
    }
    // 检查容器数合法性
    if (this.numContainers < 0) {
      throw new YarnException("numContainers becomes " + this.numContainers
          + " when adding ask " + ask + "\n requestSet: " + toString());
    }
  }

  /**
   * 将另一个资源请求集合合并到当前集合，冲突请求覆盖。
   *
   * @param requestSet 要合并的资源请求集合
   * @throws YarnException 添加请求失败时抛出异常
   */
  public void addAndOverrideRRSet(ResourceRequestSet requestSet)
      throws YarnException {
    if (requestSet == null) {
      return;
    }
    // 遍历逐个添加，自动处理覆盖
    for (ResourceRequest rr : requestSet.getRRs()) {
      addAndOverrideRR(rr);
    }
  }

  /**
   * 清理容器数为0的非ANY资源请求，避免集合过大占用内存。
   */
  public void cleanupZeroNonAnyRR() {
    Iterator<Entry<String, ResourceRequest>> iter =
        this.asks.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<String, ResourceRequest> entry = iter.next();
      // 保留ANY请求不删除
      if (entry.getKey().equals(ResourceRequest.ANY)) {
        // Do not delete ANY RR
        continue;
      }
      // 删除容器数为0的已取消位置请求
      if (entry.getValue().getNumContainers() == 0) {
        iter.remove();
      }
    }
  }

  public Map<String, ResourceRequest> getAsks() {
    return this.asks;
  }

  public Collection<ResourceRequest> getRRs() {
    return this.asks.values();
  }

  public int getNumContainers() {
    return this.numContainers;
  }

  /**
   * 强制设置当前集合需要请求的容器总数，会修改对应资源请求对象。
   *
   * @param newValue 新的容器总数
   * @throws YarnException 对取消请求设置或找不到ANY请求时抛出异常
   */
  public void setNumContainers(int newValue) throws YarnException {
    // 不允许对已取消的请求集合修改容器数
    if (this.numContainers == 0) {
      throw new YarnException("should not set numContainers to " + newValue
          + " for a cancel requestSet: " + toString());
    }

    // Clone the ResourceRequest object whenever we need to change it
    int oldValue = this.numContainers;
    this.numContainers = newValue;
    // 机会型执行类型，所有请求都需要更新容器数
    if (this.key.getExeType().equals(ExecutionType.OPPORTUNISTIC)) {
      // The assumption we made about O asks is that all RR in a requestSet has
      // the same numContainers value
      Map<String, ResourceRequest> newAsks = new HashMap<>();
      // 克隆并更新所有请求，避免修改原对象
      for (ResourceRequest rr : this.asks.values()) {
        ResourceRequest clone = ResourceRequest.clone(rr);
        clone.setNumContainers(newValue);
        newAsks.put(clone.getResourceName(), clone);
      }
      this.asks = newAsks;
    } else {
      // 保证型执行类型，仅更新ANY请求的容器数
      ResourceRequest rr = this.asks.get(ResourceRequest.ANY);
      if (rr == null) {
        throw new YarnException(
            "No ANY RR found in requestSet with numContainers=" + oldValue);
      }
      // 克隆原请求后修改，避免影响原对象
      ResourceRequest clone = ResourceRequest.clone(rr);
      clone.setNumContainers(newValue);
      this.asks.put(ResourceRequest.ANY, clone);
    }
  }

  /**
   * 获取ANY层级请求是否允许松弛位置匹配。
   *
   * @return ANY层级是否允许松弛位置匹配
   */
  public boolean isANYRelaxable() {
    return this.relaxable;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{" + this.key.toString());
    for (Entry<String, ResourceRequest> entry : this.asks.entrySet()) {
      builder.append(
          " " + entry.getValue().getNumContainers() + ":" + entry.getKey());
    }
    builder.append("}");
    return builder.toString();
  }
}