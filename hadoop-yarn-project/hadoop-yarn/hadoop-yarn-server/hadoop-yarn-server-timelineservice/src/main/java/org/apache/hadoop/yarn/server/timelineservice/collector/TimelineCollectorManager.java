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


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时间线收集器管理器，负责管理所有应用收集器的生命周期（添加/移除/启停），
 * 提供线程安全的收集器访问能力，同时负责定时持久化写入时间线数据。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TimelineCollectorManager extends CompositeService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineCollectorManager.class);

  private TimelineWriter writer;
  private ScheduledExecutorService writerFlusher;
  private int flushInterval;
  private boolean writerFlusherRunning;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 创建时间线写入器实例
    writer = createTimelineWriter(conf);
    // 初始化写入器
    writer.init(conf);
    // 创建单线程定时任务线程池，用于定期刷新写入器缓冲区
    writerFlusher = Executors.newSingleThreadScheduledExecutor();
    // 从配置读取刷新间隔（单位：秒）
    flushInterval = conf.getInt(
        YarnConfiguration.
        TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS,
        YarnConfiguration.
        DEFAULT_TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS);
    super.serviceInit(conf);
  }

  /**
   * 根据配置创建时间线写入器实例，支持自定义实现类。
   */
  private TimelineWriter createTimelineWriter(final Configuration conf) {
    // 从配置获取写入器实现类名
    String timelineWriterClassName = conf.get(
        YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WRITER_CLASS);
    LOG.info("Using TimelineWriter: {}", timelineWriterClassName);
    try {
      Class<?> timelineWriterClazz = Class.forName(timelineWriterClassName);
      if (TimelineWriter.class.isAssignableFrom(timelineWriterClazz)) {
        // 通过反射实例化写入器
        return (TimelineWriter) ReflectionUtils.newInstance(
            timelineWriterClazz, conf);
      } else {
        throw new YarnRuntimeException("Class: " + timelineWriterClassName
            + " not instance of " + TimelineWriter.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate TimelineWriter: "
          + timelineWriterClassName, e);
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    if (writer != null) {
      // 启动时间线写入器
      writer.start();
    }
    // 启动定时刷新任务，按固定间隔执行
    writerFlusher.scheduleAtFixedRate(new WriterFlushTask(writer),
        flushInterval, flushInterval, TimeUnit.SECONDS);
    // 标记刷新线程已运行
    writerFlusherRunning = true;
  }

  // 应用ID到对应收集器的映射，使用同步包装保证线程安全
  private final Map<ApplicationId, TimelineCollector> collectors =
      Collections.synchronizedMap(
          new HashMap<ApplicationId, TimelineCollector>());

  public TimelineCollectorManager(String name) {
    super(name);
  }

  protected TimelineWriter getWriter() {
    return writer;
  }

  /**
   * 如果应用对应收集器不存在，则添加新收集器到集合中。
   * 会完成收集器的初始化、启动流程。
   *
   * @param appId 应用ID
   * @param collector 待添加的时间线收集器
   * @throws YarnRuntimeException 收集器初始化启动失败时抛出
   * @return 添加后集合中对应应用的收集器（可能是已存在的）
   */
  public TimelineCollector putIfAbsent(ApplicationId appId,
      TimelineCollector collector) {
    TimelineCollector collectorInTable = null;
    synchronized (collectors) {
      collectorInTable = collectors.get(appId);
      if (collectorInTable == null) {
        try {
          // 初始化收集器，设置写入器并启动服务，后续父服务关闭时会统一清理
          collector.init(getConfig());
          collector.setWriter(writer);
          collector.start();
          collectors.put(appId, collector);
          LOG.info("the collector for {} was added", appId);
          collectorInTable = collector;
          // 添加完成后的回调处理
          postPut(appId, collectorInTable);
        } catch (Exception e) {
          throw new YarnRuntimeException(e);
        }
      } else {
        LOG.info("the collector for {} already exists!", appId);
      }
    }
    return collectorInTable;
  }

  /**
   * 收集器添加到集合后的回调处理方法。
   * @param appId 应用ID
   * @param collector 已添加的时间线收集器
   */
  public void postPut(ApplicationId appId, TimelineCollector collector) {
    doPostPut(appId, collector);
    // 标记收集器已就绪，可以开始聚合数据
    collector.setReadyToAggregate();
  }

  /**
   * 模板方法，供子类扩展实现添加后的自定义逻辑。
   * @param appId 应用ID
   * @param collector 已添加的时间线收集器
   */
  protected void doPostPut(ApplicationId appId, TimelineCollector collector) {
  }

  /**
   * 移除指定应用的收集器，并停止收集器服务。
   * 如果收集器不存在则不做任何操作。
   *
   * @param appId 待移除的应用ID
   * @return 是否成功移除
   */
  public boolean remove(ApplicationId appId) {
    TimelineCollector collector = collectors.remove(appId);
    if (collector == null) {
      LOG.error("the collector for {} does not exist!", appId);
    } else {
      synchronized (collector) {
        // 移除后的回调处理
        postRemove(appId, collector);
        // 停止服务执行清理
        collector.stop();
      }
      LOG.info("The collector service for {} was removed", appId);
    }
    return collector != null;
  }

  protected void postRemove(ApplicationId appId, TimelineCollector collector) {

  }

  /**
   * 根据应用ID获取对应的时间线收集器。
   *
   * @param appId 应用ID
   * @return 对应收集器，不存在则返回null
   */
  public TimelineCollector get(ApplicationId appId) {
    return collectors.get(appId);
  }

  /**
   * 判断指定应用是否已有对应的收集器。
   * @param appId 应用ID
   * @return 存在返回true，否则返回false
   */
  public boolean containsTimelineCollector(ApplicationId appId) {
    return collectors.containsKey(appId);
  }

  @Override
  protected void serviceStop() throws Exception {
    // 先停止所有收集器服务
    if (collectors != null && collectors.size() > 0) {
      synchronized (collectors) {
        for (TimelineCollector c : collectors.values()) {
          c.serviceStop();
        }
      }
    }
    // 优先停止定时刷新线程
    if (writerFlusher != null) {
      writerFlusher.shutdown();
      writerFlusherRunning = false;
      if (!writerFlusher.awaitTermination(30, TimeUnit.SECONDS)) {
        // 即使超时也继续关闭写入器，大部分写入器可以处理这种情况
        LOG.warn("failed to stop the flusher task in time. " +
            "will still proceed to close the writer.");
      }
    }
    // 关闭时间线写入器
    if (writer != null) {
      writer.close();
    }
    super.serviceStop();
  }

  @VisibleForTesting
  boolean writerFlusherRunning() {
    return writerFlusherRunning;
  }

  /**
   * 定时任务，负责触发时间线写入器的刷新操作，将缓冲区数据持久化。
   */
  private static class WriterFlushTask implements Runnable {
    private final TimelineWriter writer;

    public WriterFlushTask(TimelineWriter writer) {
      this.writer = writer;
    }

    public void run() {
      try {
        // 对写入器对象加锁，避免与同步写入请求冲突，保证数据一致性
        synchronized (writer) {
          writer.flush();
        }
      } catch (Throwable th) {
        // 捕获所有异常，避免异常导致后续定时任务被取消
        LOG.error("exception during timeline writer flush!", th);
      }
    }
  }
}