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

package org.apache.hadoop.yarn.server.timeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.webapp.BadRequestException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：时间线数据管理层，封装时间线存储和ACL权限管理，
 * 在写入/读取时间线数据前后进行数据预处理，并对用户访问进行权限校验。
 *
 * The class wrap over the timeline store and the ACLs manager. It does some non
 * trivial manipulation of the timeline data before putting or after getting it
 * from the timeline store, and checks the user's access to it.
 *
 */
public class TimelineDataManager extends AbstractService {

  private static final Logger LOG =
          LoggerFactory.getLogger(TimelineDataManager.class);
  @VisibleForTesting
  public static final String DEFAULT_DOMAIN_ID = "DEFAULT";

  private TimelineDataManagerMetrics metrics;
  private TimelineStore store;
  private TimelineACLsManager timelineACLsManager;

  /**
   * 构造时间线数据管理器，依赖存储层和权限管理器
   * @param store 时间线存储实例
   * @param timelineACLsManager 时间线ACL权限管理器
   */
  public TimelineDataManager(TimelineStore store,
      TimelineACLsManager timelineACLsManager) {
    super(TimelineDataManager.class.getName());
    this.store = store;
    this.timelineACLsManager = timelineACLsManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 初始化指标收集器
    metrics = TimelineDataManagerMetrics.create();
    // 获取默认域名
    TimelineDomain domain = store.getDomain("DEFAULT");
    // it is okay to reuse an existing domain even if it was created by another
    // user of the timeline server before, because it allows everybody to access.
    if (domain == null) {
      // create a default domain, which allows everybody to access and modify
      // the entities in it.
      domain = new TimelineDomain();
      domain.setId(DEFAULT_DOMAIN_ID);
      domain.setDescription("System Default Domain");
      domain.setOwner(
          UserGroupInformation.getCurrentUser().getShortUserName());
      domain.setReaders("*");
      domain.setWriters("*");
      store.put(domain);
    }
    super.serviceInit(conf);
  }

  /**
   * ACL权限检查接口
   */
  public interface CheckAcl {
    boolean check(TimelineEntity entity) throws IOException;
  }

  /**
   * ACL权限检查实现类
   */
  class CheckAclImpl implements CheckAcl {
    final UserGroupInformation ugi;

    public CheckAclImpl(UserGroupInformation callerUGI) {
      ugi = callerUGI;
    }

    public boolean check(TimelineEntity entity) throws IOException {
      try{
        return timelineACLsManager.checkAccess(
          ugi, ApplicationAccessType.VIEW_APP, entity);
      } catch (YarnException e) {
        LOG.info("Error when verifying access for user " + ugi
          + " on the events of the timeline entity "
          + new EntityIdentifier(entity.getEntityId(),
          entity.getEntityType()), e);
        return false;
      }
    }
  }

  /**
   * 获取当前用户有权限访问的时间线实体列表
   * 参数含义参考 {@link TimelineReader#getEntities}
   *
   * @see TimelineReader#getEntities
   */
  public TimelineEntities getEntities(
      String entityType,
      NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilter,
      Long windowStart,
      Long windowEnd,
      String fromId,
      Long fromTs,
      Long limit,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetEntitiesOps();
    try {
      TimelineEntities entities = doGetEntities(
          entityType,
          primaryFilter,
          secondaryFilter,
          windowStart,
          windowEnd,
          fromId,
          fromTs,
          limit,
          fields,
          callerUGI);
      metrics.incrGetEntitiesTotal(entities.getEntities().size());
      return entities;
    } finally {
      metrics.addGetEntitiesTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineEntities doGetEntities(
      String entityType,
      NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilter,
      Long windowStart,
      Long windowEnd,
      String fromId,
      Long fromTs,
      Long limit,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEntities entities = null;
    entities = store.getEntities(
        entityType,
        limit,
        windowStart,
        windowEnd,
        fromId,
        fromTs,
        primaryFilter,
        secondaryFilter,
        fields,
        new CheckAclImpl(callerUGI));

    if (entities == null) {
      return new TimelineEntities();
    }
    return entities;
  }

  /**
   * 获取单个时间线实体，检查用户访问权限
   * 参数含义参考 {@link TimelineReader#getEntity}
   *
   * @see TimelineReader#getEntity
   */
  public TimelineEntity getEntity(
      String entityType,
      String entityId,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetEntityOps();
    try {
      return doGetEntity(entityType, entityId, fields, callerUGI);
    } finally {
      metrics.addGetEntityTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineEntity doGetEntity(
      String entityType,
      String entityId,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEntity entity = null;
    entity =
        store.getEntity(entityId, entityType, fields);
    if (entity != null) {
      // 实体无domainId时设置默认domain
      addDefaultDomainIdIfAbsent(entity);
      // 检查ACL权限
      if (!timelineACLsManager.checkAccess(
          callerUGI, ApplicationAccessType.VIEW_APP, entity)) {
        final String user = callerUGI != null ? callerUGI.getShortUserName():
            null;
        throw new YarnException(
            user + " is not allowed to get the timeline entity "
            + "{ id: " + entity.getEntityId() + ", type: "
            + entity.getEntityType() + " }.");
      }
    }
    return entity;
  }

  /**
   * 获取当前用户有权限访问的实体事件列表
   * 参数含义参考 {@link TimelineReader#getEntityTimelines}
   *
   * @see TimelineReader#getEntityTimelines
   */
  public TimelineEvents getEvents(
      String entityType,
      SortedSet<String> entityIds,
      SortedSet<String> eventTypes,
      Long windowStart,
      Long windowEnd,
      Long limit,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetEventsOps();
    try {
      TimelineEvents events = doGetEvents(
          entityType,
          entityIds,
          eventTypes,
          windowStart,
          windowEnd,
          limit,
          callerUGI);
      metrics.incrGetEventsTotal(events.getAllEvents().size());
      return events;
    } finally {
      metrics.addGetEventsTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineEvents doGetEvents(
      String entityType,
      SortedSet<String> entityIds,
      SortedSet<String> eventTypes,
      Long windowStart,
      Long windowEnd,
      Long limit,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEvents events = null;
    events = store.getEntityTimelines(
        entityType,
        entityIds,
        limit,
        windowStart,
        windowEnd,
        eventTypes);
    if (events != null) {
      // 遍历每个实体的事件，过滤无权限的实体
      Iterator<TimelineEvents.EventsOfOneEntity> eventsItr =
          events.getAllEvents().iterator();
      while (eventsItr.hasNext()) {
        TimelineEvents.EventsOfOneEntity eventsOfOneEntity = eventsItr.next();
        try {
          // 获取实体信息用于权限检查
          TimelineEntity entity = store.getEntity(
              eventsOfOneEntity.getEntityId(),
              eventsOfOneEntity.getEntityType(),
              EnumSet.of(Field.PRIMARY_FILTERS));
          addDefaultDomainIdIfAbsent(entity);
          // check ACLs
          if (!timelineACLsManager.checkAccess(
              callerUGI, ApplicationAccessType.VIEW_APP, entity)) {
            // 无权限则移除该实体的事件
            eventsItr.remove();
          }
        } catch (Exception e) {
          LOG.warn("Error when verifying access for user " + callerUGI
              + " on the events of the timeline entity "
              + new EntityIdentifier(eventsOfOneEntity.getEntityId(),
                  eventsOfOneEntity.getEntityType()), e);
          // 异常时移除该实体的事件
          eventsItr.remove();
        }
      }
    }
    if (events == null) {
      return new TimelineEvents();
    }
    return events;
  }

  /**
   * 将时间线实体写入存储，设置当前用户为实体所有者，并进行权限校验
   * @param entities 待写入的实体列表
   * @param callerUGI 调用用户信息
   * @return 写入响应，包含错误信息
   * @throws YarnException
   * @throws IOException
   */
  public TimelinePutResponse postEntities(
      TimelineEntities entities,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrPostEntitiesOps();
    try {
      return doPostEntities(entities, callerUGI);
    } finally {
      metrics.addPostEntitiesTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelinePutResponse doPostEntities(
      TimelineEntities entities,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    if (entities == null) {
      return new TimelinePutResponse();
    }
    metrics.incrPostEntitiesTotal(entities.getEntities().size());
    TimelineEntities entitiesToPut = new TimelineEntities();
    List<TimelinePutResponse.TimelinePutError> errors =
        new ArrayList<TimelinePutResponse.TimelinePutError>();
    // 遍历每个待写入实体，逐个做预处理和权限校验
    for (TimelineEntity entity : entities.getEntities()) {

      // if the domain id is not specified, the entity will be put into
      // the default domain
      // 未指定domain时使用默认domain
      if (entity.getDomainId() == null ||
          entity.getDomainId().length() == 0) {
        entity.setDomainId(DEFAULT_DOMAIN_ID);
      }
      // 检查实体ID和类型完整性
      if (entity.getEntityId() == null || entity.getEntityType() == null) {
        throw new BadRequestException("Incomplete entity without entity"
            + " id/type");
      }
      // check if there is existing entity
      TimelineEntity existingEntity = null;
      try {
        // 查询已有实体，做domain一致性检查
        existingEntity =
            store.getEntity(entity.getEntityId(), entity.getEntityType(),
                EnumSet.of(Field.PRIMARY_FILTERS));
        if (existingEntity != null) {
          addDefaultDomainIdIfAbsent(existingEntity);
          // 不允许修改已有实体的domain
          if (!existingEntity.getDomainId().equals(entity.getDomainId())) {
            throw new YarnException("The domain of the timeline entity "
              + "{ id: " + entity.getEntityId() + ", type: "
              + entity.getEntityType() + " } is not allowed to be changed from "
              + existingEntity.getDomainId() + " to " + entity.getDomainId());
          }
        }
        // 检查修改权限
        if (!timelineACLsManager.checkAccess(
            callerUGI, ApplicationAccessType.MODIFY_APP, entity)) {
          throw new YarnException(callerUGI
              + " is not allowed to put the timeline entity "
              + "{ id: " + entity.getEntityId() + ", type: "
              + entity.getEntityType() + " } into the domain "
              + entity.getDomainId() + ".");
        }
      } catch (Exception e) {
        // Skip the entity which already exists and was put by others
        LOG.warn("Skip the timeline entity: { id: " + entity.getEntityId()
            + ", type: "+ entity.getEntityType() + " }", e);
        // 记录错误，跳过该实体
        TimelinePutResponse.TimelinePutError error =
            new TimelinePutResponse.TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(
            TimelinePutResponse.TimelinePutError.ACCESS_DENIED);
        errors.add(error);
        continue;
      }

      entitiesToPut.addEntity(entity);
    }

    // 批量写入通过校验的实体
    TimelinePutResponse response = store.put(entitiesToPut);
    // 合并预校验阶段的错误
    response.addErrors(errors);
    return response;
  }

  /**
   * 新增或更新domain，仅允许所有者或管理员修改已有domain
   * @param domain 待写入的domain
   * @param callerUGI 调用用户信息
   * @throws YarnException
   * @throws IOException
   */
  public void putDomain(TimelineDomain domain,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrPutDomainOps();
    try {
      doPutDomain(domain, callerUGI);
    } finally {
      metrics.addPutDomainTime(Time.monotonicNow() - startTime);
    }
  }

  private void doPutDomain(TimelineDomain domain,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    // 查询已有domain
    TimelineDomain existingDomain =
        store.getDomain(domain.getId());
    if (existingDomain != null) {
      // 已有domain检查修改权限
      if (!timelineACLsManager.checkAccess(callerUGI, existingDomain)) {
        throw new YarnException(callerUGI.getShortUserName() +
            " is not allowed to override an existing domain " +
            existingDomain.getId());
      }
      // 保留原有所有者，ACL关闭时仍然可以修改但不改变所有者
      domain.setOwner(existingDomain.getOwner());
    }
    // 写入domain
    store.put(domain);
    // If the domain exists already, it is likely to be in the cache.
    // We need to invalidate it.
    if (existingDomain != null) {
      // 更新缓存中的domain信息，使权限变更立即生效
      timelineACLsManager.replaceIfExist(domain);
    }
  }

  /**
   * 获取单个domain，无权限时返回null
   * @param domainId domain ID
   * @param callerUGI 调用用户信息
   * @return domain实例，无权限则返回null
   * @throws YarnException
   * @throws IOException
   */
  public TimelineDomain getDomain(String domainId,
      UserGroupInformation callerUG