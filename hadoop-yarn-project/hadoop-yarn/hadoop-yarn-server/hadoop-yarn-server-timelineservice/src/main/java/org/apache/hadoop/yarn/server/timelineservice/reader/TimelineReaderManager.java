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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;

/**
 * 时间线数据读取管理器，封装底层时间线存储读取实现，在从后端存储获取数据前后对时间线数据进行预处理和后处理。
 */
@Private
@Unstable
public class TimelineReaderManager extends AbstractService {

  private TimelineReader reader;
  private AdminACLsManager adminACLsManager;

  /**
   * 构造函数，基于传入的时间线读取器创建管理器实例。
   * @param timelineReader 底层时间线读取器实例
   */
  public TimelineReaderManager(TimelineReader timelineReader) {
    super(TimelineReaderManager.class.getName());
    this.reader = timelineReader;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // TODO ACL功能完善后需要修改或移除这段代码
    this.adminACLsManager = new AdminACLsManager(conf);
    super.serviceInit(conf);
  }

  /**
   * 若客户端未提供集群ID，则从配置中读取yarn.resourcemanager.cluster-id返回。
   * @param clusterId 客户端传入的集群ID
   * @param conf 配置对象
   * @return 最终有效的集群ID
   */
  private static String getClusterID(String clusterId, Configuration conf) {
    if (clusterId == null || clusterId.isEmpty()) {
      return conf.get(
          YarnConfiguration.RM_CLUSTER_ID,
              YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
    }
    return clusterId;
  }

  /**
   * 将字符串类型的实体类型转换为枚举类型，转换失败返回null。
   * @param entityType 字符串形式的实体类型
   * @return 对应的枚举类型实例或null
   */
  private static TimelineEntityType getTimelineEntityType(String entityType) {
    if (entityType == null) {
      return null;
    }
    try {
      return TimelineEntityType.valueOf(entityType);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * 根据实体类型和查询上下文，在时间线实体的info字段中填充唯一标识ID(UID)。
   * @param entityType 查询的实体类型枚举
   * @param entity 时间线实体对象
   * @param context 查询上下文对象
   */
  private static void fillUID(TimelineEntityType entityType,
      TimelineEntity entity, TimelineReaderContext context) {
    if (entityType != null) {
      switch(entityType) {
      case YARN_FLOW_ACTIVITY:
        // 处理流活动实体，填充上下文并生成对应UID
        FlowActivityEntity activityEntity = (FlowActivityEntity)entity;
        context.setUserId(activityEntity.getUser());
        context.setFlowName(activityEntity.getFlowName());
        entity.setUID(TimelineReaderUtils.UID_KEY,
            TimelineUIDConverter.FLOW_UID.encodeUID(context));
        return;
      case YARN_FLOW_RUN:
        // 处理流运行实体，填充上下文并生成对应UID
        FlowRunEntity runEntity = (FlowRunEntity)entity;
        context.setFlowRunId(runEntity.getRunId());
        entity.setUID(TimelineReaderUtils.UID_KEY,
            TimelineUIDConverter.FLOWRUN_UID.encodeUID(context));
        return;
      case YARN_APPLICATION:
        // 处理YARN应用实体，填充上下文并生成对应UID
        context.setAppId(entity.getId());
        entity.setUID(TimelineReaderUtils.UID_KEY,
            TimelineUIDConverter.APPLICATION_UID.encodeUID(context));
        return;
      default:
        break;
      }
    }
    // 处理通用实体，根据是否有代理用户生成不同类型的UID
    context.setEntityType(entity.getType());
    context.setEntityIdPrefix(entity.getIdPrefix());
    context.setEntityId(entity.getId());
    if (context.getDoAsUser() != null) {
      entity.setUID(TimelineReaderUtils.UID_KEY,
          TimelineUIDConverter.SUB_APPLICATION_ENTITY_UID.encodeUID(context));
    } else {
      entity.setUID(TimelineReaderUtils.UID_KEY,
          TimelineUIDConverter.GENERIC_ENTITY_UID.encodeUID(context));
    }
  }

  /**
   * 根据查询条件从后端存储获取匹配的实体集合，自动补全集群ID并为每个实体生成对应UID。
   *
   * @param context 时间线查询上下文，限定获取实体的范围
   * @param filters 过滤条件，限制返回实体数量
   * @param dataToRetrieve 指定每个实体需要返回的数据部分
   * @return 匹配条件的时间线实体集合
   * @throws IOException 从后端存储读取数据失败时抛出
   * @see TimelineReader#getEntities
   */
  public Set<TimelineEntity> getEntities(TimelineReaderContext context,
      TimelineEntityFilters filters, TimelineDataToRetrieve dataToRetrieve)
      throws IOException {
    // 补全集群ID
    context.setClusterId(getClusterID(context.getClusterId(), getConfig()));
    // 调用底层读取器获取实体
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext(context), filters, dataToRetrieve);
    // 为每个返回实体填充UID
    if (entities != null) {
      TimelineEntityType type = getTimelineEntityType(context.getEntityType());
      for (TimelineEntity entity : entities) {
        fillUID(type, entity, context);
      }
    }
    return entities;
  }

  /**
   * 根据查询条件从后端存储获取单个时间线实体，自动补全集群ID并生成对应UID。
   *
   * @param context 时间线查询上下文，限定获取实体的范围
   * @param dataToRetrieve 指定实体需要返回的数据部分
   * @return 找到的时间线实体，未找到则返回null
   * @throws IOException 从后端存储读取数据失败时抛出
   * @see TimelineReader#getEntity
   */
  public TimelineEntity getEntity(TimelineReaderContext context,
      TimelineDataToRetrieve dataToRetrieve) throws IOException {
    // 补全集群ID
    context.setClusterId(
        getClusterID(context.getClusterId(), getConfig()));
    // 调用底层读取器获取实体
    TimelineEntity entity = reader.getEntity(
        new TimelineReaderContext(context), dataToRetrieve);
    // 为返回实体填充UID
    if (entity != null) {
      TimelineEntityType type = getTimelineEntityType(context.getEntityType());
      fillUID(type, entity, context);
    }
    return entity;
  }

  /**
   * 获取应用下所有可用的时间线实体类型，自动补全缺失的集群ID。
   *
   * @param context 时间线查询上下文，实体类型字段应为null
   * @return 包含所有可用实体类型字符串的集合，未找到则返回空集合
   * @throws IOException 从后端存储读取数据失败时抛出
   */
  public Set<String> getEntityTypes(TimelineReaderContext context)
      throws IOException{
    // 补全集群ID
    context.setClusterId(getClusterID(context.getClusterId(), getConfig()));
    // 调用底层读取器获取实体类型
    return reader.getEntityTypes(context);
  }

  /**
   * 检查当前用户是否拥有时间线数据读取权限。
   * @param callerUGI 调用用户的用户组信息
   * @return 允许访问返回true，否则返回false
   */
  public boolean checkAccess(UserGroupInformation callerUGI) {
    // TODO ACL功能完善后需要修改或移除这段代码
    if (!adminACLsManager.areACLsEnabled()) {
      return true;
    }
    return callerUGI != null && adminACLsManager.isAdmin(callerUGI);
  }

  /**
   * 获取时间线读取器的健康状态，检查连接是否正常。
   *
   * @return 健康状态对象，连接正常返回true，否则返回false
   */
  public TimelineHealth getHealthStatus() {
    return reader.getHealthStatus();
  }
}