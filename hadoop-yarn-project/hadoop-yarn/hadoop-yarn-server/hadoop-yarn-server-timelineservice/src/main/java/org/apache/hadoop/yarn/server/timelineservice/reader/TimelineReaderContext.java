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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.TimelineContext;

/**
 * 时间线服务查询上下文，封装时间线阅读器执行查询所需的所有参数信息。
 * 继承自TimelineContext，扩展了实体相关查询参数。
 */
@Private
@Unstable
public class TimelineReaderContext extends TimelineContext {

  /** 实体类型 */
  private String entityType;
  /** 实体ID */
  private String entityId;
  /** 实体ID前缀，用于前缀匹配查询 */
  private Long entityIdPrefix;
  /** 代理执行查询的用户，用于权限检查 */
  private String doAsUser;
  /** 是否为通用实体，标识查询是否针对通用时间线实体 */
  private boolean genericEntity = false;

  /**
   * 构造函数，初始化基本查询上下文参数。
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 流名称
   * @param flowRunId 流运行ID
   * @param appId 应用ID
   * @param entityType 实体类型
   * @param entityId 实体ID
   */
  public TimelineReaderContext(String clusterId, String userId, String flowName,
      Long flowRunId, String appId, String entityType, String entityId) {
    super(clusterId, userId, flowName, flowRunId, appId);
    this.entityType = entityType;
    this.entityId = entityId;
  }

  /**
   * 构造函数，增加实体ID前缀参数。
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 流名称
   * @param flowRunId 流运行ID
   * @param appId 应用ID
   * @param entityType 实体类型
   * @param entityIdPrefix 实体ID前缀
   * @param entityId 实体ID
   */
  public TimelineReaderContext(String clusterId, String userId, String flowName,
      Long flowRunId, String appId, String entityType, Long entityIdPrefix,
      String entityId) {
    this(clusterId, userId, flowName, flowRunId, appId, entityType, entityId);
    this.entityIdPrefix = entityIdPrefix;
  }

  /**
   * 构造函数，增加代理用户参数。
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 流名称
   * @param flowRunId 流运行ID
   * @param appId 应用ID
   * @param entityType 实体类型
   * @param entityIdPrefix 实体ID前缀
   * @param entityId 实体ID
   * @param doasUser 代理执行用户
   */
  public TimelineReaderContext(String clusterId, String userId, String flowName,
      Long flowRunId, String appId, String entityType, Long entityIdPrefix,
      String entityId, String doasUser) {
    this(clusterId, userId, flowName, flowRunId, appId, entityType, entityId);
    this.entityIdPrefix = entityIdPrefix;
    this.doAsUser = doasUser;
  }

  /**
   * 构造函数，增加通用实体标识参数。
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 流名称
   * @param flowRunId 流运行ID
   * @param appId 应用ID
   * @param entityType 实体类型
   * @param entityIdPrefix 实体ID前缀
   * @param entityId 实体ID
   * @param doasUser 代理执行用户
   * @param genericEntity 是否为通用实体
   */
  public TimelineReaderContext(String clusterId, String userId, String flowName,
      Long flowRunId, String appId, String entityType, Long entityIdPrefix,
      String entityId, String doasUser, boolean genericEntity) {
    this(clusterId, userId, flowName, flowRunId, appId, entityType,
        entityIdPrefix, entityId, doasUser);
    this.genericEntity = genericEntity;
  }

  /**
   * 拷贝构造函数，基于另一个TimelineReaderContext创建新实例。
   * @param other 待拷贝的上下文对象
   */
  public TimelineReaderContext(TimelineReaderContext other) {
    this(other.getClusterId(), other.getUserId(), other.getFlowName(),
        other.getFlowRunId(), other.getAppId(), other.getEntityType(),
        other.getEntityIdPrefix(), other.getEntityId(), other.getDoAsUser(),
        other.genericEntity);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    // 叠加实体ID前缀哈希
    result = prime * result
        + ((entityIdPrefix == null) ? 0 : entityIdPrefix.hashCode());
    // 叠加实体ID哈希
    result = prime * result + ((entityId == null) ? 0 : entityId.hashCode());
    // 叠加实体类型哈希
    result =
        prime * result + ((entityType == null) ? 0 : entityType.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    // 同一对象直接返回相等
    if (this == obj) {
      return true;
    }
    // 父类不相等则整体不相等
    if (!super.equals(obj)) {
      return false;
    }
    // 类型转换后比较各字段
    TimelineReaderContext other = (TimelineReaderContext) obj;
    // 比较实体ID
    if (entityId == null) {
      if (other.entityId != null) {
        return false;
      }
    } else if (!entityId.equals(other.entityId)) {
      return false;
    }
    // 比较实体类型
    if (entityType == null) {
      if (other.entityType != null) {
        return false;
      }
    } else if (!entityType.equals(other.entityType)) {
      return false;
    }
    return true;
  }

  /**
   * 获取实体类型。
   * @return 实体类型字符串
   */
  public String getEntityType() {
    return entityType;
  }

  /**
   * 设置实体类型。
   * @param type 实体类型字符串
   */
  public void setEntityType(String type) {
    this.entityType = type;
  }

  /**
   * 获取实体ID。
   * @return 实体ID字符串
   */
  public String getEntityId() {
    return entityId;
  }

  /**
   * 设置实体ID。
   * @param id 实体ID字符串
   */
  public void setEntityId(String id) {
    this.entityId = id;
  }

  /**
   * 获取实体ID前缀。
   * @return 实体ID前缀
   */
  public Long getEntityIdPrefix() {
    return entityIdPrefix;
  }

  /**
   * 设置实体ID前缀。
   * @param entityIdPrefix 实体ID前缀
   */
  public void setEntityIdPrefix(Long entityIdPrefix) {
    this.entityIdPrefix = entityIdPrefix;
  }

  /**
   * 获取代理执行用户。
   * @return 代理用户名
   */
  public String getDoAsUser() {
    return doAsUser;
  }

  /**
   * 设置代理执行用户。
   * @param doAsUser 代理用户名
   */
  public void setDoAsUser(String doAsUser) {
    this.doAsUser = doAsUser;
  }

  /**
   * 判断是否为通用实体查询。
   * @return true表示查询通用实体，false表示查询流关联实体
   */
  public boolean isGenericEntity() {
    return genericEntity;
  }

  /**
   * 设置通用实体查询标识。
   * @param genericEntity 是否为通用实体
   */
  public void setGenericEntity(boolean genericEntity) {
    this.genericEntity = genericEntity;
  }

}