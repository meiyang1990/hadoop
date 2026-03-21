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

package org.apache.hadoop.yarn.server.resourcemanager.ahs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.applicationhistoryservice.NullApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件级注释：RM应用历史写入服务，ResourceManager通过该类异步持久化应用、应用尝试、容器的生命周期信息到历史存储
 * <p>
 * {@link ResourceManager} uses this class to write the information of
 * {@link RMApp}, {@link RMAppAttempt} and {@link RMContainer}. These APIs are
 * non-blocking, and just schedule a writing history event. An self-contained
 * dispatcher vector will handle the event in separate threads, and extract the
 * required fields that are going to be persisted. Then, the extracted
 * information will be persisted via the implementation of
 * {@link ApplicationHistoryStore}.
 * </p>
 */
@Private
@Unstable
public class RMApplicationHistoryWriter extends CompositeService {

  /** 日志记录器 */
  public static final Logger LOG =
      LoggerFactory.getLogger(RMApplicationHistoryWriter.class);

  /** 事件分发器，负责处理异步写入事件 */
  private Dispatcher dispatcher;
  @VisibleForTesting
  /** 应用历史写入器实例，最终持久化操作委托给它 */
  ApplicationHistoryWriter writer;
  @VisibleForTesting
  /** 应用历史服务是否启用标志 */
  boolean historyServiceEnabled;

  /** 构造函数，初始化复合服务 */
  public RMApplicationHistoryWriter() {
    super(RMApplicationHistoryWriter.class.getName());
  }

  @Override
  protected synchronized void serviceInit(Configuration conf) throws Exception {
    // 从配置读取应用历史服务是否启用
    historyServiceEnabled =
        conf.getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
          YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED);
    // 如果未配置历史存储或配置为空存储，则禁用历史服务
    if (conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE) == null ||
        conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE).length() == 0 ||
        conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE).equals(
            NullApplicationHistoryStore.class.getName())) {
      historyServiceEnabled = false;
    }

    // Only create the services when the history service is enabled and not
    // using the null store, preventing wasting the system resources.
    // 仅在启用历史服务时初始化相关服务，避免浪费系统资源
    if (historyServiceEnabled) {
      // 创建应用历史存储实例
      writer = createApplicationHistoryStore(conf);
      // 将存储实例添加到复合服务管理
      addIfService(writer);
  
      // 创建多线程事件分发器
      dispatcher = createDispatcher(conf);
      // 注册历史写入事件处理器
      dispatcher.register(WritingHistoryEventType.class,
        new ForwardingEventHandler());
      // 将分发器添加到复合服务管理
      addIfService(dispatcher);
    }
    super.serviceInit(conf);
  }

  /**
   * 创建多线程事件分发器，用于异步处理历史写入事件
   * @param conf 配置对象
   * @return 分发器实例
   */
  protected Dispatcher createDispatcher(Configuration conf) {
    MultiThreadedDispatcher dispatcher =
        new MultiThreadedDispatcher(
          conf
            .getInt(
              YarnConfiguration.RM_HISTORY_WRITER_MULTI_THREADED_DISPATCHER_POOL_SIZE,
              YarnConfiguration.DEFAULT_RM_HISTORY_WRITER_MULTI_THREADED_DISPATCHER_POOL_SIZE));
    // 设置停止时排空所有待处理事件，保证数据不丢失
    dispatcher.setDrainEventsOnStop();
    return dispatcher;
  }

  /**
   * 从配置反射创建应用历史存储实例
   * @param conf 配置对象
   * @return 应用历史存储实例
   */
  protected ApplicationHistoryStore createApplicationHistoryStore(
      Configuration conf) {
    try {
      // 从配置获取存储实现类
      Class<? extends ApplicationHistoryStore> storeClass =
          conf.getClass(YarnConfiguration.APPLICATION_HISTORY_STORE,
              NullApplicationHistoryStore.class,
              ApplicationHistoryStore.class);
      // 实例化存储对象
      return storeClass.newInstance();
    } catch (Exception e) {
      String msg =
          "Could not instantiate ApplicationHistoryWriter: "
              + conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE,
                  NullApplicationHistoryStore.class.getName());
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }

  /**
   * 处理不同类型的历史写入事件，将事件数据持久化到存储
   * @param event 历史写入事件
   */
  protected void handleWritingApplicationHistoryEvent(
      WritingApplicationHistoryEvent event) {
    switch (event.getType()) {
      case APP_START:
        // 应用启动事件
        WritingApplicationStartEvent wasEvent =
            (WritingApplicationStartEvent) event;
        try {
          // 持久化应用启动数据
          writer.applicationStarted(wasEvent.getApplicationStartData());
          LOG.info("Stored the start data of application "
              + wasEvent.getApplicationId());
        } catch (IOException e) {
          LOG.error("Error when storing the start data of application "
              + wasEvent.getApplicationId());
        }
        break;
      case APP_FINISH:
        // 应用完成事件
        WritingApplicationFinishEvent wafEvent =
            (WritingApplicationFinishEvent) event;
        try {
          // 持久化应用完成数据
          writer.applicationFinished(wafEvent.getApplicationFinishData());
          LOG.info("Stored the finish data of application "
              + wafEvent.getApplicationId());
        } catch (IOException e) {
          LOG.error("Error when storing the finish data of application "
              + wafEvent.getApplicationId());
        }
        break;
      case APP_ATTEMPT_START:
        // 应用尝试启动事件
        WritingApplicationAttemptStartEvent waasEvent =
            (WritingApplicationAttemptStartEvent) event;
        try {
          // 持久化应用尝试启动数据
          writer.applicationAttemptStarted(waasEvent
            .getApplicationAttemptStartData());
          LOG.info("Stored the start data of application attempt "
              + waasEvent.getApplicationAttemptId());
        } catch (IOException e) {
          LOG.error("Error when storing the start data of application attempt "
              + waasEvent.getApplicationAttemptId());
        }
        break;
      case APP_ATTEMPT_FINISH:
        // 应用尝试完成事件
        WritingApplicationAttemptFinishEvent waafEvent =
            (WritingApplicationAttemptFinishEvent) event;
        try {
          // 持久化应用尝试完成数据
          writer.applicationAttemptFinished(waafEvent
            .getApplicationAttemptFinishData());
          LOG.info("Stored the finish data of application attempt "
              + waafEvent.getApplicationAttemptId());
        } catch (IOException e) {
          LOG
            .error("Error when storing the finish data of application attempt "
                + waafEvent.getApplicationAttemptId());
        }
        break;
      case CONTAINER_START:
        // 容器启动事件
        WritingContainerStartEvent wcsEvent =
            (WritingContainerStartEvent) event;
        try {
          // 持久化容器启动数据
          writer.containerStarted(wcsEvent.getContainerStartData());
          LOG.info("Stored the start data of container "
              + wcsEvent.getContainerId());
        } catch (IOException e) {
          LOG.error("Error when storing the start data of container "
              + wcsEvent.getContainerId());
        }
        break;
      case CONTAINER_FINISH:
        // 容器完成事件
        WritingContainerFinishEvent wcfEvent =
            (WritingContainerFinishEvent) event;
        try {
          // 持久化容器完成数据
          writer.containerFinished(wcfEvent.getContainerFinishData());
          LOG.info("Stored the finish data of container "
              + wcfEvent.getContainerId());
        } catch (IOException e) {
          LOG.error("Error when storing the finish data of container "
              + wcfEvent.getContainerId());
        }
        break;
      default:
        // 未知事件类型
        LOG.error("Unknown WritingApplicationHistoryEvent type: "
            + event.getType());
    }
  }

  /**
   * 提交应用启动事件，异步写入应用启动历史
   * @param app RM应用实例
   */
  @SuppressWarnings("unchecked")
  public void applicationStarted(RMApp app) {
    if (historyServiceEnabled) {
      dispatcher.getEventHandler().handle(
        new WritingApplicationStartEvent(app.getApplicationId(),
          ApplicationStartData.newInstance(app.getApplicationId(), app.getName(),
            app.getApplicationType(), app.getQueue(), app.getUser(),
            app.getSubmitTime(), app.getStartTime())));
    }
  }

  /**
   * 提交应用完成事件，异步写入应用完成历史
   * @param app RM应用实例
   * @param finalState 应用最终状态
   */
  @SuppressWarnings("unchecked")
  public void applicationFinished(RMApp app, RMAppState finalState) {
    if (historyServiceEnabled) {
      dispatcher.getEventHandler().handle(
        new WritingApplicationFinishEvent(app.getApplicationId(),
          ApplicationFinishData.newInstance(app.getApplicationId(),
            app.getFinishTime(), app.getDiagnostics().toString(),
            app.getFinalApplicationStatus(),
            RMServerUtils.createApplicationState(finalState))));
    }
  }

  /**
   * 提交应用尝试启动事件，异步写入应用尝试启动历史
   * @param appAttempt RM应用尝试实例
   */
  @SuppressWarnings("unchecked")
  public void applicationAttemptStarted(RMAppAttempt appAttempt) {
    if (historyServiceEnabled) {
      dispatcher.getEventHandler().handle(
        new WritingApplicationAttemptStartEvent(appAttempt.getAppAttemptId(),
          ApplicationAttemptStartData.newInstance(appAttempt.getAppAttemptId(),
            appAttempt.getHost(), appAttempt.getRpcPort(), appAttempt
              .getMasterContainer().getId())));
    }
  }

  /**
   * 提交应用尝试完成事件，异步写入应用尝试完成历史
   * @param appAttempt RM应用尝试实例
   * @param finalState 应用尝试最终状态
   */
  @SuppressWarnings("unchecked")
  public void applicationAttemptFinished(RMAppAttempt appAttempt,
      RMAppAttemptState finalState) {
    if (historyServiceEnabled) {
      dispatcher.getEventHandler().handle(
        new WritingApplicationAttemptFinishEvent(appAttempt.getAppAttemptId(),
          ApplicationAttemptFinishData.newInstance(
            appAttempt.getAppAttemptId(), appAttempt.getDiagnostics()
              .toString(), appAttempt.getTrackingUrl(), appAttempt
              .getFinalApplicationStatus(),
              RMServerUtils.createApplicationAttemptState(finalState))));
    }
  }

  /**
   * 提交容器启动事件，异步写入容器启动历史
   * @param container RM容器实例
   */
  @SuppressWarnings("unchecked")
  public void containerStarted(RMContainer container) {
    if (historyServiceEnabled) {
      dispatcher.getEventHandler().handle(
        new WritingContainerStartEvent(container.getContainerId(),
          ContainerStartData.newInstance(container.getContainerId(),
            container.getAllocatedResource(), container.getAllocatedNode(),
            container.getAllocatedPriority(), container.getCreationTime())));
    }
  }

  /**
   * 提交容器完成事件，异步写入容器完成历史
   * @param container RM容器实例
   */
  @SuppressWarnings("unchecked")
  public void containerFinished(RMContainer container) {
    if (historyServiceEnabled) {
      dispatcher.getEventHandler().handle(
        new WritingContainerFinishEvent(container.getContainerId(),
          ContainerFinishData.newInstance(container.getContainerId(),
            container.getFinishTime(), container.getDiagnosticsInfo(),
            container.getContainerExitStatus(),
            container.getContainerState())));
    }
  }

  /**
   * 事件转发处理器，将收到的事件转发给handleWritingApplicationHistoryEvent处理
   */
  private final class ForwardingEventHandler implements
      EventHandler<WritingApplicationHistoryEvent> {

    @Override
    public void handle(WritingApplicationHistoryEvent event) {
      handleWritingApplicationHistoryEvent(event);
    }

  }

  /**
   * 多线程事件分发器，通过多个异步分发器并行处理不同应用的历史写入事件
   * 保证同一应用的事件按顺序处理，提升写入吞吐量
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected static class MultiThreadedDispatcher extends CompositeService
      implements Dispatcher {

    /** 异步分发器列表，每个分发器对应一个处理线程 */
    private List<AsyncDispatcher> dispatchers =
        new ArrayList<AsyncDispatcher>();

    /**
     * 构造函数，创建指定数量的异步分发器
     * @param num 分发器数量（即处理线程数）
     */
    public MultiThreadedDispatcher(int num) {
      super(MultiThreadedDispatcher.class.getName());
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
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
      // 给所有子分发器注册相同的事件处理器
      for (AsyncDispatcher dispatcher : dispatchers) {
        dispatcher.register(eventType, handler);
      }
    }

    /**
     * 设置所有子分发器停止时排空事件
     */
    public void setDrainEventsOnStop() {
      for (AsyncDispatcher dispatcher : dispatchers) {
        dispatcher.setDrainEventsOnStop();
      }
    }

    /**
     * 复合事件处理器，根据事件哈希分发到不同子分发器
     * 保证同一应用的所有事件路由到同一个分发器，保持事件顺序
     */
    private class CompositEventHandler implements EventHandler<Event> {

      @Override
      public void handle(Event event) {
        // Use hashCode (of ApplicationId) to dispatch the event to the child
        // dispatcher, such that all the writing events of one application will
        // be handled by one thread, the scheduled order of the these events
        // will be preserved
        // 根据事件哈希值取模，选择对应分发器，保证同一应用事件顺序
        int index = (event.hashCode() & Integer.MAX_VALUE) % dispatchers.size();
        dispatchers.get(index).getEventHandler().handle(event);
      }

    }

    /**
     * 创建单个异步分发器实例
     * @return 异步分发器
     */
    protected AsyncDispatcher createDispatcher() {
      return new AsyncDispatcher("RM ApplicationHistory dispatcher");
    }

  }

}