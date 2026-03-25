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
package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;

/**
 * 系统指标发布器抽象基类，供不同版本时间线服务的具体发布器实现扩展，提供多线程事件分发基础能力
 */
public abstract class AbstractSystemMetricsPublisher extends CompositeService
    implements SystemMetricsPublisher {
  private MultiThreadedDispatcher dispatcher;

  protected Dispatcher getDispatcher() {
    return dispatcher;
  }

  public AbstractSystemMetricsPublisher(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置读取线程池大小，使用默认值兜底
    dispatcher =
    new MultiThreadedDispatcher(getConfig().getInt(
        YarnConfiguration.
        RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE,
        YarnConfiguration.
        DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE));
    // 设置停止时排空所有待处理事件
    dispatcher.setDrainEventsOnStop();
    // 将分发器添加为服务
    addIfService(dispatcher);
    super.serviceInit(conf);
  }

  /**
   * 多线程事件分发器，用于并行处理ATS相关指标事件
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static class MultiThreadedDispatcher extends CompositeService
      implements Dispatcher {

    private List<AsyncDispatcher> dispatchers =
        new ArrayList<AsyncDispatcher>();

    public MultiThreadedDispatcher(int num) {
      super(MultiThreadedDispatcher.class.getName());
      // 按指定数量创建异步分发器
      for (int i = 0; i < num; ++i) {
        AsyncDispatcher dispatcher = createDispatcher();
        dispatchers.add(dispatcher);
        addIfService(dispatcher);
      }
    }

    @Override
    public EventHandler<Event> getEventHandler() {
      return new CompositEventHandler();
    }

    @Override
    public void register(Class<? extends Enum> eventType,
        EventHandler handler) {
      // 向所有子分发器注册同一事件处理器
      for (AsyncDispatcher dispatcher : dispatchers) {
        dispatcher.register(eventType, handler);
      }
    }

    public void setDrainEventsOnStop() {
      // 对所有子分发器设置停止排空
      for (AsyncDispatcher dispatcher : dispatchers) {
        dispatcher.setDrainEventsOnStop();
      }
    }

    /**
     * 复合事件处理器，按应用ID哈希分发事件到对应线程
     */
    private class CompositEventHandler implements EventHandler<Event> {

      @Override
      public void handle(Event event) {
        // 根据事件哈希选择子分发器，保证同一个应用的所有事件都由同一个线程处理
        // 维持应用事件的发布顺序一致性
        int index = (event.hashCode() & Integer.MAX_VALUE) % dispatchers.size();
        dispatchers.get(index).getEventHandler().handle(event);
      }
    }

    protected AsyncDispatcher createDispatcher() {
      return new AsyncDispatcher("RM Timeline dispatcher");
    }
  }

  /**
   * 系统指标事件类型定义
   */
  protected enum SystemMetricsEventType {
    /** 发布实体事件 */
    PUBLISH_ENTITY,
    /** 发布应用完成实体事件 */
    PUBLISH_APPLICATION_FINISHED_ENTITY
  }

  @Override
  public void appLaunched(RMApp app, long launchTime) {
  }

  /**
   * 时间线发布事件抽象基类，重写哈希方法保证同应用事件分发到同一线程
   */
  protected static abstract class TimelinePublishEvent
      extends AbstractEvent<SystemMetricsEventType> {

    private ApplicationId appId;

    public TimelinePublishEvent(SystemMetricsEventType type,
        ApplicationId appId) {
      super(type);
      this.appId = appId;
    }

    public ApplicationId getApplicationId() {
      return appId;
    }

    @Override
    public int hashCode() {
      // 基于应用ID计算哈希，保证同应用事件哈希一致
      return appId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof TimelinePublishEvent)) {
        return false;
      }
      TimelinePublishEvent other = (TimelinePublishEvent) obj;
      if (appId == null) {
        if (other.appId != null) {
          return false;
        }
      } else if (getType() == null) {
        if (other.getType() != null) {
          return false;
        }
      } else {
        if (!appId.equals(other.appId) || !getType().equals(other.getType())) {
          return false;
        }
      }
      return true;
    }
  }
}