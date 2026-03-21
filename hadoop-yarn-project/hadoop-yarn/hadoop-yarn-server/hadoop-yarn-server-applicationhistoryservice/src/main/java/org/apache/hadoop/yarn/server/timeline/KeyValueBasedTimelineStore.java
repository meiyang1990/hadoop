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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager.CheckAcl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import static org.apache.hadoop.yarn.server.timeline.TimelineDataManager.DEFAULT_DOMAIN_ID;
import static org.apache.hadoop.yarn.server.timeline.TimelineStoreMapAdapter.CloseableIterator;

/**
 * 基于键值对存储的时间线存储抽象实现，依赖TimelineStoreMapAdapter对接具体的键值存储
 * 所有公共方法使用synchronized保证线程安全，避免并发修改问题
 */
@Private
@Unstable
abstract class KeyValueBasedTimelineStore
    extends AbstractService implements TimelineStore {

  /** 时间线实体存储：通过实体标识映射实体对象 */
  protected TimelineStoreMapAdapter<EntityIdentifier, TimelineEntity> entities;
  /** 实体插入时间存储：记录每个实体的写入时间，用于分页查询 */
  protected TimelineStoreMapAdapter<EntityIdentifier, Long> entityInsertTimes;
  /** 域存储：通过域ID映射域对象 */
  protected TimelineStoreMapAdapter<String, TimelineDomain> domainById;
  /** 域按所有者分组存储：通过所有者用户名映射该用户拥有的所有域 */
  protected TimelineStoreMapAdapter<String, Set<TimelineDomain>> domainsByOwner;

  private boolean serviceStopped = false;

  private static final Logger LOG
      = LoggerFactory.getLogger(KeyValueBasedTimelineStore.class);

  public KeyValueBasedTimelineStore() {
    super(KeyValueBasedTimelineStore.class.getName());
  }

  public KeyValueBasedTimelineStore(String name) {
    super(name);
  }

  /**
   * 获取服务是否已停止
   * @return 服务停止状态
   */
  public synchronized boolean getServiceStopped() {
    return serviceStopped;
  }

  @Override
  protected synchronized void serviceStop() throws Exception {
    serviceStopped = true;
    super.serviceStop();
  }

  @Override
  /**
   * 根据查询条件获取符合条件的时间线实体列表
   */
  public synchronized TimelineEntities getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, String fromId, Long fromTs,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields, CheckAcl checkAcl) throws IOException {
    // 服务已停止直接返回空
    if (getServiceStopped()) {
      LOG.info("Service stopped, return null for the storage");
      return null;
    }
    // 设置默认查询参数
    if (limit == null) {
      limit = DEFAULT_LIMIT;
    }
    if (windowStart == null) {
      windowStart = Long.MIN_VALUE;
    }
    if (windowEnd == null) {
      windowEnd = Long.MAX_VALUE;
    }
    if (fields == null) {
      fields = EnumSet.allOf(Field.class);
    }

    // 处理分页起始点
    TimelineEntity firstEntity = null;
    if (fromId != null) {
      firstEntity = entities.get(new EntityIdentifier(fromId,
          entityType));
      if (firstEntity == null) {
        return new TimelineEntities();
      }
    }

    List<TimelineEntity> entitiesSelected = new ArrayList<TimelineEntity>();

    // 遍历实体迭代器，应用过滤条件
    try(CloseableIterator<TimelineEntity> entityIterator =
        firstEntity == null ? entities.valueSetIterator() :
            entities.valueSetIterator(firstEntity)) {
      while (entityIterator.hasNext()) {
        TimelineEntity entity = entityIterator.next();
        // 达到数量限制停止遍历
        if (entitiesSelected.size() >= limit) {
          break;
        }
        // 过滤不匹配实体类型的实体
        if (!entity.getEntityType().equals(entityType)) {
          continue;
        }
        // 过滤超出时间窗口范围的实体
        if (entity.getStartTime() <= windowStart) {
          continue;
        }
        if (entity.getStartTime() > windowEnd) {
          continue;
        }
        // 过滤插入时间晚于起始时间戳的实体（分页）
        if (fromTs != null && entityInsertTimes.get(
            new EntityIdentifier(entity.getEntityId(), entity.getEntityType()))
            > fromTs) {
          continue;
        }
        // 应用主过滤条件
        if (primaryFilter != null && !KeyValueBasedTimelineStoreUtils
            .matchPrimaryFilter(entity.getPrimaryFilters(), primaryFilter)) {
          continue;
        }
        // 应用多个二级过滤条件（AND逻辑，全部满足才保留）
        if (secondaryFilters != null) {
          boolean flag = true;
          for (NameValuePair secondaryFilter : secondaryFilters) {
            if (secondaryFilter != null && !KeyValueBasedTimelineStoreUtils
                .matchPrimaryFilter(entity.getPrimaryFilters(), secondaryFilter)
                && !KeyValueBasedTimelineStoreUtils
                .matchFilter(entity.getOtherInfo(), secondaryFilter)) {
              flag = false;
              break;
            }
          }
          if (!flag) {
            continue;
          }
        }
        // 设置默认域ID
        if (entity.getDomainId() == null) {
          entity.setDomainId(DEFAULT_DOMAIN_ID);
        }
        // ACL检查通过后加入结果集
        if (checkAcl == null || checkAcl.check(entity)) {
          entitiesSelected.add(entity);
        }
      }
    }

    // 按要求过滤返回字段，排序后返回
    List<TimelineEntity> entitiesToReturn = new ArrayList<TimelineEntity>();
    for (TimelineEntity entitySelected : entitiesSelected) {
      entitiesToReturn.add(KeyValueBasedTimelineStoreUtils.maskFields(
          entitySelected, fields));
    }
    Collections.sort(entitiesToReturn);
    TimelineEntities entitiesWrapper = new TimelineEntities();
    entitiesWrapper.setEntities(entitiesToReturn);
    return entitiesWrapper;
  }

  @Override
  /**
   * 根据实体ID和类型获取单个时间线实体
   */
  public synchronized TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fieldsToRetrieve) {
    if (getServiceStopped()) {
      LOG.info("Service stopped, return null for the storage");
      return null;
    }
    if (fieldsToRetrieve == null) {
      fieldsToRetrieve = EnumSet.allOf(Field.class);
    }
    TimelineEntity
        entity = entities.get(new EntityIdentifier(entityId, entityType));
    if (entity == null) {
      return null;
    } else {
      // 按要求过滤返回字段
      return KeyValueBasedTimelineStoreUtils.maskFields(
          entity, fieldsToRetrieve);
    }
  }

  @Override
  /**
   * 获取多个实体指定时间范围的事件列表
   */
  public synchronized TimelineEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd,
      Set<String> eventTypes) {
    if (getServiceStopped()) {
      LOG.info("Service stopped, return null for the storage");
      return null;
    }
    TimelineEvents allEvents = new TimelineEvents();
    if (entityIds == null) {
      return allEvents;
    }
    // 设置默认查询参数
    if (limit == null) {
      limit = DEFAULT_LIMIT;
    }
    if (windowStart == null) {
      windowStart = Long.MIN_VALUE;
    }
    if (windowEnd == null) {
      windowEnd = Long.MAX_VALUE;
    }
    // 遍历每个实体，收集符合条件的事件
    for (String entityId : entityIds) {
      EntityIdentifier entityID = new EntityIdentifier(entityId, entityType);
      TimelineEntity entity = entities.get(entityID);
      if (entity == null) {
        continue;
      }
      EventsOfOneEntity events = new EventsOfOneEntity();
      events.setEntityId(entityId);
      events.setEntityType(entityType);
      for (TimelineEvent event : entity.getEvents()) {
        // 达到数量限制停止
        if (events.getEvents().size() >= limit) {
          break;
        }
        // 过滤超出时间窗口范围的事件
        if (event.getTimestamp() <= windowStart) {
          continue;
        }
        if (event.getTimestamp() > windowEnd) {
          continue;
        }
        // 过滤不匹配事件类型的事件
        if (eventTypes != null && !eventTypes.contains(event.getEventType())) {
          continue;
        }
        events.addEvent(event);
      }
      allEvents.addEvent(events);
    }
    return allEvents;
  }

  @Override
  /**
   * 根据域ID获取域信息
   */
  public TimelineDomain getDomain(String domainId)
      throws IOException {
    if (getServiceStopped()) {
      LOG.info("Service stopped, return null for the storage");
      return null;
    }
    TimelineDomain domain = domainById.get(domainId);
    if (domain == null) {
      return null;
    } else {
      // 复制域信息返回
      return KeyValueBasedTimelineStoreUtils.createTimelineDomain(
          domain.getId(),
          domain.getDescription(),
          domain.getOwner(),
          domain.getReaders(),
          domain.getWriters(),
          domain.getCreatedTime(),
          domain.getModifiedTime());
    }
  }

  @Override
  /**
   * 根据所有者获取该用户拥有的所有域，按创建/修改时间倒序排序
   */
  public TimelineDomains getDomains(String owner)
      throws IOException {
    if (getServiceStopped()) {
      LOG.info("Service stopped, return null for the storage");
      return null;
    }
    List<TimelineDomain> domains = new ArrayList<TimelineDomain>();
    Set<TimelineDomain> domainsOfOneOwner = domainsByOwner.get(owner);
    if (domainsOfOneOwner == null) {
      return new TimelineDomains();
    }
    // 复制每个域信息
    for (TimelineDomain domain : domainsByOwner.get(owner)) {
      TimelineDomain domainToReturn = KeyValueBasedTimelineStoreUtils
          .createTimelineDomain(
              domain.getId(),
              domain.getDescription(),
              domain.getOwner(),
              domain.getReaders(),
              domain.getWriters(),
              domain.getCreatedTime(),
              domain.getModifiedTime());
      domains.add(domainToReturn);
    }
    // 按创建时间倒序，创建时间相同则按修改时间倒序排序
    Collections.sort(domains, new Comparator<TimelineDomain>() {
      @Override
      public int compare(
          TimelineDomain domain1, TimelineDomain domain2) {
         int result = domain2.getCreatedTime().compareTo(
             domain1.getCreatedTime());
         if (result == 0) {
           return domain2.getModifiedTime().compareTo(
               domain1.getModifiedTime());
         } else {
           return result;
         }
      }
    });
    TimelineDomains domainsToReturn = new TimelineDomains();
    domainsToReturn.addDomains(domains);
    return domainsToReturn;
  }

  @Override
  /**
   * 批量写入时间线实体，增量更新已有实体信息
   */
  public synchronized TimelinePutResponse put(TimelineEntities data) {
    TimelinePutResponse response = new TimelinePutResponse();
    if (getServiceStopped()) {
      LOG.info("Service stopped, return null for the storage");
      TimelinePutError error = new TimelinePutError();
      error.setErrorCode(TimelinePutError.IO_EXCEPTION);
      response.addError(error);
      return response;
    }
    // 逐个处理每个实体
    for (TimelineEntity entity : data.getEntities()) {
      EntityIdentifier entityId =
          new EntityIdentifier(entity.getEntityId(), entity.getEntityType());
      // 获取已有实体，不存在则新建
      TimelineEntity existingEntity = entities.get(entityId);
      boolean needsPut = false;
      if (existingEntity == null) {
        existingEntity = new TimelineEntity();
        existingEntity.setEntityId(entity.getEntityId());
        existingEntity.setEntityType(entity.getEntityType());
        existingEntity.setStartTime(entity.getStartTime());
        // 新实体必须指定域ID
        if (entity.getDomainId() == null ||
            entity.getDomainId().length() == 0) {
          TimelinePutError error = new TimelinePutError();
          error.setEntityId(entityId.getId());
          error.setEntityType(entityId.getType());
          error.setErrorCode(TimelinePutError.NO_DOMAIN);
          response.addError(error);
          continue;
        }
        existingEntity.setDomainId(entity.getDomainId());
        // 记录新实体插入时间
        entityInsertTimes.put(entityId, System.currentTimeMillis());
        needsPut = true;
      }
      // 合并事件，合并后排序
      if (entity.getEvents() != null) {
        if (existingEntity.getEvents() == null) {
          existingEntity.setEvents(entity.getEvents());
        } else {
          existingEntity.addEvents(entity.getEvents());
        }
        Collections.sort(existingEntity.getEvents());
        needsPut = true;
      }
      // 如果实体没有设置开始时间，从事件中提取最小时间戳作为开始时间
      if (existingEntity.getStartTime() == null) {
        if (existingEntity.getEvents() == null
            || existingEntity.getEvents().isEmpty()) {
          TimelinePutError error = new TimelinePutError();
          error.setEntityId(entityId.getId());
          error.setEntityType(entityId.getType());
          error.setErrorCode(TimelinePutError.NO_START_TIME);
          response.addError(error);
          entities.remove(entityId);
          entityInsertTimes.remove(entityId);
          continue;
        } else {
          Long min = Long.MAX_VALUE;
          for (TimelineEvent e : entity.getEvents()) {
            if (min > e.getTimestamp()) {
              min = e.getTimestamp();
            }
          }
          existingEntity.setStartTime(min);
          needsPut = true;
        }
      }
      // 合并主过滤条件
      if (entity.getPrimaryFilters() != null) {
        if (existingEntity.getPrimaryFilters() == null) {
          existingEntity.setPrimaryFilters(new HashMap<String, Set<Object>>());
        }
        for (Entry<String, Set<Object>> pf :
            entity.getPrimaryFilters().entrySet()) {
          for (Object pfo : pf.getValue()) {
            existingEntity.addPrimaryFilter(pf.getKey(),
                KeyValueBasedTimelineStoreUtils.compactNumber(pfo));
            needsPut = true;
          }
        }
      }
      // 合并其他信息
      if (entity.getOtherInfo() != null) {
        if (existingEntity.getOtherInfo() == null) {
          existingEntity.setOtherInfo(new HashMap<String, Object>());
        }
        for (Entry<String, Object> info : entity.getOtherInfo().entrySet()) {
          existingEntity.addOtherInfo(info.getKey(),
              KeyValueBasedTimelineStoreUtils.compactNumber(info.getValue()));
          needsPut = true;
        }
      }
      // 如果有修改，写入存储
      if (needsPut) {
        entities.put(entityId, existingEntity);
      }

      // 处理关联实体关系
      if (entity.getRelatedEntities() == null) {
        continue;
      }
      for (Entry<String, Set<String>> partRelatedEntities : entity
          .getRelatedEntities().entrySet()) {
        if (partRelatedEntities == null) {
          continue;
        }
        for (String idStr : partRelatedEntities.getValue()) {
          Entity