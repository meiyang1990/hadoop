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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时间线服务数据采集器抽象服务，负责处理时间线数据写入并持久化到后端存储。
 * 子类可扩展生命周期管理或自定义请求处理逻辑。
 */
@Private
@Unstable
public abstract class TimelineCollector extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineCollector.class);
  /** 分隔符，用于聚合后指标ID拼接 */
  public static final String SEPARATOR = "_";

  private TimelineWriter writer;
  // 按实体类型分组的聚合状态表
  private ConcurrentMap<String, AggregationStatusTable> aggregationGroups
      = new ConcurrentHashMap<>();
  private static Set<String> entityTypesSkipAggregation
      = new HashSet<>();
  // 异步写入线程池
  private ThreadPoolExecutor pool;

  // 标记是否已准备好进行指标聚合
  private volatile boolean readyToAggregate = false;

  // 标记服务是否已停止
  private volatile boolean isStopped = false;

  // 写入失败最大重试次数
  private int maxWriteRetries;
  // 重试间隔（毫秒）
  private long writeRetryInterval;

  public TimelineCollector(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 从配置读取异步写入队列容量
    int capacity = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_WRITER_ASYNC_QUEUE_CAPACITY,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WRITER_ASYNC_QUEUE_CAPACITY
    );
    // 初始化单线程线程池，处理异步写入任务
    pool = new ThreadPoolExecutor(1, 1, 3, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(capacity));
    // 队列满时丢弃最旧任务
    pool.setRejectedExecutionHandler(
        new ThreadPoolExecutor.DiscardOldestPolicy());

    // 读取配置：写入最大重试次数
    maxWriteRetries =
        conf.getInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_MAX_RETRIES);
    // 读取配置：重试间隔
    writeRetryInterval = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;
    // 立即关闭线程池，中断正在执行的任务
    pool.shutdownNow();
    super.serviceStop();
  }

  boolean isStopped() {
    return isStopped;
  }

  protected void setWriter(TimelineWriter w) {
    this.writer = w;
  }

  protected Map<String, AggregationStatusTable> getAggregationGroups() {
    return aggregationGroups;
  }

  protected void setReadyToAggregate() {
    readyToAggregate = true;
  }

  protected boolean isReadyToAggregate() {
    return readyToAggregate;
  }

  /**
   * 获取需要跳过聚合的时间线实体类型集合，子类可覆盖自定义行为。
   *
   * @return 需要跳过聚合的实体类型集合
   */
  protected Set<String> getEntityTypesSkipAggregation() {
    return entityTypesSkipAggregation;
  }

  public abstract TimelineCollectorContext getTimelineEntityContext();


  /**
   * 同步处理实体写入，直接写入后端存储不缓冲批量处理。
   * 已存在的实体会执行更新操作，仅用于关键实体写入，常规大量写入推荐使用异步方法。
   *
   * @param entities 待写入的实体集合
   * @param callerUgi 调用者用户信息
   * @return 写入响应结果
   * @throws IOException 写入过程中发生异常抛出
   */
  public TimelineWriteResponse putEntities(TimelineEntities entities,
      UserGroupInformation callerUgi) throws IOException {
    LOG.debug("putEntities(entities={}, callerUgi={})", entities, callerUgi);

    TimelineWriteResponse response = null;
    try {
      // 重试检查存储可用性，等待存储恢复
      boolean isStorageUp = checkRetryWithSleep();
      if (isStorageUp) {
        // 对writer加锁，避免并发刷新缓冲导致异常吞掉本次写入的错误
        synchronized (writer) {
          // 写入时间线实体
          response = writeTimelineEntities(entities, callerUgi);
          // 刷新缓冲数据到存储
          flushBufferedTimelineEntities();
        }
      } else {
        String msg = String.format("Failed to putEntities(" +
            "entities=%s, callerUgi=%s) as Timeline Storage is Down",
            entities, callerUgi);
        throw new IOException(msg);
      }
    } catch (InterruptedException ex) {
      String msg = String.format("Interrupted while retrying to putEntities(" +
          "entities=%s, callerUgi=%s)", entities, callerUgi);
      throw new IOException(msg);
    }

    return response;
  }


  /**
   * 重复检查存储健康状态，失败后间隔重试，直到重试次数耗尽。
   * @return 存储正常返回true，重试耗尽仍失败返回false
   * @throws InterruptedException 睡眠过程中被中断抛出
   */
  private boolean checkRetryWithSleep() throws InterruptedException {
    int retries = maxWriteRetries;
    while (retries > 0) {
      TimelineHealth timelineHealth = writer.getHealthStatus();
      if (timelineHealth.getHealthStatus().equals(
              TimelineHealth.TimelineHealthStatus.RUNNING)) {
        return true;
      } else {
        try {
          // 等待重试间隔后重试
          Thread.sleep(writeRetryInterval);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw ex;
        }
        retries--;
      }
    }
    return false;
  }


  /**
   * 添加或更新域名信息，已存在的域名仅允许所有者或管理员更新。
   *
   * @param domain 待写入的域名对象
   * @param callerUgi 调用者用户信息
   * @return 写入响应结果
   * @throws IOException 写入过程中发生异常抛出
   */
  public TimelineWriteResponse putDomain(TimelineDomain domain,
      UserGroupInformation callerUgi) throws IOException {
    LOG.debug("putDomain(domain={}, callerUgi={})", domain, callerUgi);

    TimelineWriteResponse response = null;
    try {
      boolean isStorageUp = checkRetryWithSleep();
      if (isStorageUp) {
        synchronized (writer) {
          final TimelineCollectorContext context = getTimelineEntityContext();
          // 写入域名到存储
          response = writer.write(context, domain);
          // 刷新缓冲数据
          flushBufferedTimelineEntities();
        }
      } else {
        String msg = String.format("Failed to putDomain(" +
            "domain=%s, callerUgi=%s) as Timeline Storage is Down",
            domain, callerUgi);
        throw new IOException(msg);
      }
    } catch (InterruptedException ex) {
      String msg = String.format("Interrupted while retrying to putDomain(" +
          "domain=%s, callerUgi=%s)", domain, callerUgi);
      throw new IOException(msg);
    }

    return response;
  }

  private TimelineWriteResponse writeTimelineEntities(
      TimelineEntities entities, UserGroupInformation callerUgi)
      throws IOException {
    // 更新聚合状态表，为实时指标聚合做准备
    updateAggregateStatus(entities, aggregationGroups,
        getEntityTypesSkipAggregation());

    final TimelineCollectorContext context = getTimelineEntityContext();
    // 调用写入器写入实体到存储
    return writer.write(context, entities, callerUgi);
  }

  /**
   * 刷新writer缓冲中的时间线实体数据到后端存储。
   * @throws IOException 刷新过程中发生IO异常抛出
   */
  private void flushBufferedTimelineEntities() throws IOException {
    writer.flush();
  }

  /**
   * 异步处理实体写入，验证通过后立即返回，写入任务交由后台线程处理。
   * 同一实体的多次写入可能会被批量合并，减少后端存储写入次数。
   *
   * @param entities 待写入的实体集合
   * @param callerUgi 调用者用户信息
   * @throws IOException 任务提交失败抛出
   */
  public void putEntitiesAsync(TimelineEntities entities,
      UserGroupInformation callerUgi) throws IOException {
    LOG.debug("putEntitiesAsync(entities={}, callerUgi={})", entities,
        callerUgi);

    // 提交异步写入任务到线程池
    pool.execute(new Runnable() {
      @Override public void run() {
        try {
          writeTimelineEntities(entities, callerUgi);
        } catch (IOException ie) {
          LOG.error("Got an exception while writing entity", ie);
        }
      }
    });
  }

  /**
   * 对给定时间线实体中的指标进行全量聚合，生成聚合结果实体。
   *
   * @param entities 待聚合的实体集合
   * @param resultEntityId 聚合结果实体ID
   * @param resultEntityType 聚合结果实体类型
   * @param needsGroupIdInResult 是否需要在聚合指标ID中包含分组ID
   * @return 包含所有聚合后指标的时间线实体
   */
  public static TimelineEntity aggregateEntities(
      TimelineEntities entities, String resultEntityId,
      String resultEntityType, boolean needsGroupIdInResult) {
    ConcurrentMap<String, AggregationStatusTable> aggregationGroups
        = new ConcurrentHashMap<>();
    updateAggregateStatus(entities, aggregationGroups, null);
    if (needsGroupIdInResult) {
      return aggregate(aggregationGroups, resultEntityId, resultEntityType);
    } else {
      return aggregateWithoutGroupId(
          aggregationGroups, resultEntityId, resultEntityType);
    }
  }

  /**
   * 更新聚合状态表，将新实体中的指标加入聚合缓存。
   *
   * @param entities 待更新的实体集合
   * @param aggregationGroups 聚合状态表
   * @param typesToSkip 需要跳过聚合的实体类型
   */
  static void updateAggregateStatus(
      TimelineEntities entities,
      ConcurrentMap<String, AggregationStatusTable> aggregationGroups,
      Set<String> typesToSkip) {
    // 遍历所有实体
    for (TimelineEntity e : entities.getEntities()) {
      // 跳过指定类型或无指标的实体
      if ((typesToSkip != null && typesToSkip.contains(e.getType()))
          || e.getMetrics().isEmpty()) {
        continue;
      }
      // 获取对应实体类型的聚合状态表
      AggregationStatusTable aggrTable = aggregationGroups.get(e.getType());
      if (aggrTable == null) {
        // 不存在则创建新表
        AggregationStatusTable table = new AggregationStatusTable();
        aggrTable = aggregationGroups.putIfAbsent(e.getType(),
            table);
        if (aggrTable == null) {
          aggrTable = table;
        }
      }
      // 更新实体指标到聚合表
      aggrTable.update(e);
    }
  }

  /**
   * 聚合所有分组状态，生成带分组ID的聚合结果实体。
   *
   * @param aggregationGroups 聚合状态表
   * @param resultEntityId 结果实体ID
   * @param resultEntityType 结果实体类型
   * @return 聚合结果实体
   */
  static TimelineEntity aggregate(
      Map<String, AggregationStatusTable> aggregationGroups,
      String resultEntityId, String resultEntityType) {
    TimelineEntity result = new TimelineEntity();
    result.setId(resultEntityId);
    result.setType(resultEntityType);
    for (Map.Entry<String, AggregationStatusTable> entry
        : aggregationGroups.entrySet()) {
      // 按分组聚合所有指标到结果实体
      entry.getValue().aggregateAllTo(result, entry.getKey());
    }
    return result;
  }

  /**
   * 聚合所有分组状态，生成不带分组ID的聚合结果实体。
   *
   * @param aggregationGroups 聚合状态表
   * @param resultEntityId 结果实体ID
   * @param resultEntityType 结果实体类型
   * @return 聚合结果实体
   */
  static TimelineEntity aggregateWithoutGroupId(
      Map<String, AggregationStatusTable> aggregationGroups,
      String resultEntityId, String resultEntityType) {
    TimelineEntity result = new TimelineEntity();
    result.setId(resultEntityId);
    result.setType(resultEntityType);
    for (Map.Entry<String, AggregationStatusTable> entry
        : aggregationGroups.entrySet()) {
      entry.getValue().aggregateAllTo(result, "");
    }
    return result;
  }

  // Note: In memory aggregation is performed in an eventually consistent
  // fashion.
  /**
   * 指标聚合状态表，内存中维护待聚合的指标状态，最终一致聚合。
   */
  protected static class AggregationStatusTable {
    // 聚合表结构：key为基础指标，value为<实体ID, 实体指标>映射表
    private ConcurrentMap<TimelineMetric, Map<String, TimelineMetric>>
        aggregateTable;

    public AggregationStatusTable() {
      aggregateTable = new ConcurrentHashMap<>();
    }

    /**
     * 将传入实体的指标更新到聚合表中。
     * @param incoming 传入时间线实体
     */
    public void update(TimelineEntity incoming) {
      String entityId = incoming.getId();
      for (TimelineMetric m : incoming.getMetrics()) {
        // 跳过不需要聚合的指标
        if (m.getRealtimeAggregationOp() == TimelineMetricOperation.NOP) {
          continue;
        }
        // 获取指标对应的聚合行
        Map<String, TimelineMetric> aggrRow = aggregateTable.get(m);
        if (aggrRow == null) {
          // 不存在则新建聚合行
          Map<String, TimelineMetric> tempRow = new HashMap<>();
          aggrRow = aggregateTable.putIfAbsent(m, tempRow);
          if (aggrRow == null) {
            aggrRow = tempRow;
          }
        }
        // 加锁更新当前实体的指标快照
        synchronized (aggrRow) {
          aggrRow.put(entityId, m);
        }
      }
    }

    /**
     * 将指定指标的所有实体快照聚合到目标实体中。
     * @param metric 待聚合的基础指标
     * @param e 目标结果实体
     * @param aggregationGroupId 聚合分组ID
     * @return 聚合后的结果实体
     */
    public TimelineEntity aggregateTo(TimelineMetric metric, TimelineEntity e,
        String aggregationGroupId) {
      if (metric.getRealtimeAggregationOp() == TimelineMetricOperation.NOP) {
        return e;
      }
      Map<String, TimelineMetric> aggrRow = aggregateTable.get(metric);
      if (aggrRow != null) {
        // 创建聚合后的指标对象
        TimelineMetric aggrMetric = new TimelineMetric();
        // 拼接指标ID，如果有分组ID
        if (aggregationGroupId.length() > 0) {
          aggr