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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * YARN调度器中对一组资源请求分组的唯一键。
 * 基于基础调度请求键扩展，增加了资源大小和执行类型维度。
 *
 * TODO: after YARN-7631 is fixed by adding Resource and ExecType into
 * SchedulerRequestKey, then we can directly use that.
 */
public class ResourceRequestSetKey extends SchedulerRequestKey {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceRequestSetKey.class);

  // 在SchedulerRequestKey基础上扩展更多资源请求键字段
  private final Resource resource;
  private final ExecutionType execType;

  /**
   * 从资源请求对象构造分组键。
   *
   * @param rr 资源请求对象
   * @throws YarnException 如果资源请求缺少必要字段时抛出
   */
  public ResourceRequestSetKey(ResourceRequest rr) throws YarnException {
    this(rr.getAllocationRequestId(), rr.getPriority(), rr.getCapability(),
        ((rr.getExecutionTypeRequest() == null) ? ExecutionType.GUARANTEED
            : rr.getExecutionTypeRequest().getExecutionType()));
    if (rr.getPriority() == null) {
      throw new YarnException("Null priority in RR: " + rr);
    }
    if (rr.getCapability() == null) {
      throw new YarnException("Null resource in RR: " + rr);
    }
  }

  /**
   * 从各维度成员构造分组键。
   *
   * @param allocationRequestId 分配请求ID
   * @param priority 资源请求优先级
   * @param resource 请求的资源量
   * @param execType 容器执行类型
   */
  public ResourceRequestSetKey(long allocationRequestId, Priority priority,
      Resource resource, ExecutionType execType) {
    super(priority, allocationRequestId, null);

    if (resource == null) {
      this.resource = Resource.newInstance(0, 0);
    } else {
      this.resource = resource;
    }
    if (execType == null) {
      this.execType = ExecutionType.GUARANTEED;
    } else {
      this.execType = execType;
    }
  }

  public Resource getResource() {
    return this.resource;
  }

  public ExecutionType getExeType() {
    return this.execType;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SchedulerRequestKey)) {
      return false;
    }
    if (!(obj instanceof ResourceRequestSetKey)) {
      return super.equals(obj);
    }
    ResourceRequestSetKey other = (ResourceRequestSetKey) obj;
    return super.equals(other) && this.resource.equals(other.resource)
        && this.execType.equals(other.execType);
  }

  @Override
  public int hashCode() {
    return ((super.hashCode() * 37 + this.resource.hashCode()) * 41)
        + this.execType.hashCode();
  }

  @Override
  public int compareTo(SchedulerRequestKey other) {
    int ret = super.compareTo(other);
    if (ret != 0) {
      return ret;
    }
    if (!(other instanceof ResourceRequestSetKey)) {
      return ret;
    }

    ResourceRequestSetKey otherKey = (ResourceRequestSetKey) other;
    ret = this.resource.compareTo(otherKey.resource);
    if (ret != 0) {
      return ret;
    }
    return this.execType.compareTo(otherKey.execType);
  }

  /**
   * 从给定键集合中提取匹配已分配容器的分组键。未找到匹配返回null。
   * 精确匹配失败时，如果分配请求ID非零，会按分配请求ID做模糊匹配。
   *
   * @param container 已分配的容器对象
   * @param keys 待查找的分组键集合
   * @return 匹配的ResourceRequestSetKey，未找到返回null
   */
  public static ResourceRequestSetKey extractMatchingKey(Container container,
      Set<ResourceRequestSetKey> keys) {
    // 基于容器信息构造待匹配分组键
    ResourceRequestSetKey resourceRequestSetKey = new ResourceRequestSetKey(
        container.getAllocationRequestId(), container.getPriority(),
        container.getResource(), container.getExecutionType());
    if (keys.contains(resourceRequestSetKey)) {
      return resourceRequestSetKey;
    }

    // 精确匹配失败，按非零分配请求ID模糊匹配
    if (container.getAllocationRequestId() > 0) {
      // If no exact match, look for the one with the same (non-zero)
      // allocationRequestId
      for (ResourceRequestSetKey candidate : keys) {
        if (candidate.getAllocationRequestId() == container.getAllocationRequestId()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Using possible match for {} : {}", resourceRequestSetKey, candidate);
          }
          return candidate;
        }
      }
    }

    // 未找到匹配，输出调试日志
    if (LOG.isDebugEnabled()) {
      LOG.debug("not match found for container {}.", container.getId());
      for (ResourceRequestSetKey candidate : keys) {
        LOG.debug("candidate set keys: {}.", candidate.toString());
      }
    }

    return null;
  }

  @Override
  public String toString() {
    return "[id:" + getAllocationRequestId() + " p:"
        + getPriority().getPriority()
        + (this.execType.equals(ExecutionType.GUARANTEED) ? " G"
            : " O" + " r:" + this.resource + "]");
  }
}