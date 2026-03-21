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

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 应用级时间线数据聚合服务，基于{@link AppLevelTimelineCollector}实现，
 * 负责将聚合后的时间线数据写入时间线服务，并管理应用相关的生命周期。
 */
@Private
@Unstable
public class AppLevelTimelineCollectorWithAgg
    extends AppLevelTimelineCollector {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineCollector.class);

  // 聚合线程池固定线程数
  private final static int AGGREGATION_EXECUTOR_NUM_THREADS = 1;
  // 聚合执行间隔（秒）
  private int aggregationExecutorIntervalSecs;
  // 需要跳过聚合的实体类型集合
  private static Set<String> entityTypesSkipAggregation
      = initializeSkipSet();

  // 应用级聚合定时线程池
  private ScheduledThreadPoolExecutor appAggregationExecutor;
  // 应用级聚合任务实例
  private AppLevelAggregator appAggregator;

  /**
   * 构造函数，创建带聚合能力的应用级时间线收集器。
   * @param appId 应用ID
   * @param user 应用对应用户
   */
  public AppLevelTimelineCollectorWithAgg(ApplicationId appId, String user) {
    super(appId, user);
  }

  /**
   * 初始化需要跳过聚合的实体类型集合。
   * @return 跳过聚合的实体类型集合
   */
  private static Set<String> initializeSkipSet() {
    Set<String> result = new HashSet<>();
    result.add(TimelineEntityType.YARN_APPLICATION.toString());
    result.add(TimelineEntityType.YARN_FLOW_RUN.toString());
    result.add(TimelineEntityType.YARN_FLOW_ACTIVITY.toString());
    return result;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置读取聚合间隔，使用默认值兜底
    aggregationExecutorIntervalSecs = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_AGGREGATION_INTERVAL_SECS,
        YarnConfiguration.
            DEFAULT_TIMELINE_SERVICE_AGGREGATION_INTERVAL_SECS
    );
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 启动聚合定时线程
    appAggregationExecutor = new ScheduledThreadPoolExecutor(
        AppLevelTimelineCollectorWithAgg.AGGREGATION_EXECUTOR_NUM_THREADS,
        new ThreadFactoryBuilder()
            .setNameFormat("TimelineCollector Aggregation thread #%d")
            .build());
    appAggregator = new AppLevelAggregator();
    // 按固定间隔调度聚合任务
    appAggregationExecutor.scheduleAtFixedRate(appAggregator,
        aggregationExecutorIntervalSecs,
        aggregationExecutorIntervalSecs,
        TimeUnit.SECONDS);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 关闭聚合线程池
    appAggregationExecutor.shutdown();
    // 等待优雅关闭超时后强制关闭
    if (!appAggregationExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
      LOG.info("App-level aggregator shutdown timed out, shutdown now. ");
      appAggregationExecutor.shutdownNow();
    }
    // 线程池关闭后执行最后一轮聚合
    appAggregator.aggregate();
    super.serviceStop();
  }

  @Override
  protected Set<String> getEntityTypesSkipAggregation() {
    return entityTypesSkipAggregation;
  }

  /**
   * 应用级时间线聚合任务，按固定间隔执行聚合。
   */
  private class AppLevelAggregator implements Runnable {

    /**
     * 执行应用级时间线指标聚合。
     */
    private void aggregate() {
      LOG.debug("App-level real-time aggregating");
      if (!isReadyToAggregate()) {
        LOG.warn("App-level collector is not ready, skip aggregation. ");
        return;
      }
      try {
        // 获取当前时间线实体上下文
        TimelineCollectorContext currContext = getTimelineEntityContext();
        // 获取所有聚合分组
        Map<String, AggregationStatusTable> aggregationGroups
            = getAggregationGroups();
        // 无聚合数据直接跳过
        if (aggregationGroups == null
            || aggregationGroups.isEmpty()) {
          LOG.debug("App-level collector is empty, skip aggregation. ");
          return;
        }
        // 执行无分组应用级聚合，生成聚合后的应用实体
        TimelineEntity resultEntity = TimelineCollector.aggregateWithoutGroupId(
            aggregationGroups, currContext.getAppId(),
            TimelineEntityType.YARN_APPLICATION.toString());
        // 异步写入聚合结果到时间线服务
        TimelineEntities entities = new TimelineEntities();
        entities.addEntity(resultEntity);
        putEntitiesAsync(entities, getCurrentUser());
      } catch (Exception e) {
        LOG.error("Error aggregating timeline metrics", e);
      }
      LOG.debug("App-level real-time aggregation complete");
    }

    @Override
    public void run() {
      aggregate();
    }
  }

}