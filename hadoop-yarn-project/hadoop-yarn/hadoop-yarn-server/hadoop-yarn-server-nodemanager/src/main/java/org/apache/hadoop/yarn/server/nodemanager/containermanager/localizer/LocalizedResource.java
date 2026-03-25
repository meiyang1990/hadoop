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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceFailedLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRecoveredEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/**
 * 表示一个已本地化或正在本地化的资源，维护资源本地化的状态机，跟踪使用该资源的容器。
 * 资源状态定义在{@link ResourceState}枚举中。
 */
public class LocalizedResource implements EventHandler<ResourceEvent> {

  private static final Logger LOG =
       LoggerFactory.getLogger(LocalizedResource.class);

  // 资源本地化后的本地路径
  volatile Path localPath;
  // 资源大小，单位字节，-1表示未知
  volatile long size = -1;
  // 资源请求描述信息
  final LocalResourceRequest rsrc;
  // 事件分发器，用于发布事件通知
  final Dispatcher dispatcher;
  // 资源状态机实例
  final StateMachine<ResourceState,ResourceEventType,ResourceEvent>
    stateMachine;
  // 信号量，控制本地化流程的并发访问
  final Semaphore sem = new Semaphore(1);
  // 使用该资源的容器队列，记录引用计数
  final Queue<ContainerId> ref;
  // 读写锁，保护状态和容器列表的并发访问
  private final Lock readLock;
  private final Lock writeLock;

  // 资源最近被引用的时间戳（用于缓存清理）
  final AtomicLong timestamp = new AtomicLong(currentTime());

  // 资源状态机工厂，定义所有合法的状态转移规则
  private static final StateMachineFactory<LocalizedResource,ResourceState,
      ResourceEventType,ResourceEvent> stateMachineFactory =
        new StateMachineFactory<LocalizedResource,ResourceState,
          ResourceEventType,ResourceEvent>(ResourceState.INIT)

    // 初始状态INIT，引用计数为0，等待请求
    .addTransition(ResourceState.INIT, ResourceState.DOWNLOADING,
        ResourceEventType.REQUEST, new FetchResourceTransition())
    .addTransition(ResourceState.INIT, ResourceState.LOCALIZED,
        ResourceEventType.RECOVERED, new RecoveredTransition())

    // DOWNLOADING状态，已有请求，正在本地化中
    .addTransition(ResourceState.DOWNLOADING, ResourceState.DOWNLOADING,
        ResourceEventType.REQUEST, new FetchResourceTransition()) // TODO: Duplicate addition!!
    .addTransition(ResourceState.DOWNLOADING, ResourceState.LOCALIZED,
        ResourceEventType.LOCALIZED, new FetchSuccessTransition())
    .addTransition(ResourceState.DOWNLOADING,ResourceState.DOWNLOADING,
        ResourceEventType.RELEASE, new ReleaseTransition())
    .addTransition(ResourceState.DOWNLOADING, ResourceState.FAILED,
        ResourceEventType.LOCALIZATION_FAILED, new FetchFailedTransition())

    // LOCALIZED状态，本地化完成，已存储在本地磁盘
    .addTransition(ResourceState.LOCALIZED, ResourceState.LOCALIZED,
        ResourceEventType.REQUEST, new LocalizedResourceTransition())
    .addTransition(ResourceState.LOCALIZED, ResourceState.LOCALIZED,
        ResourceEventType.RELEASE, new ReleaseTransition())
    .installTopology();

  /**
   * 构造一个待本地化的资源实例。
   * @param rsrc 资源请求描述
   * @param dispatcher 事件分发器
   */
  public LocalizedResource(LocalResourceRequest rsrc, Dispatcher dispatcher) {
    this.rsrc = rsrc;
    this.dispatcher = dispatcher;
    this.ref = new LinkedList<ContainerId>();

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ ").append(rsrc.toString()).append(",")
      .append(getState() == ResourceState.LOCALIZED
          ? getLocalPath() + "," + getSize()
          : "pending").append(",[");
    this.readLock.lock();
    try {
      for (ContainerId c : ref) {
        sb.append("(").append(c.toString()).append(")");
      }
      sb.append("],").append(getTimestamp()).append(",").append(getState())
        .append("}");
      return sb.toString();
    } finally {
      this.readLock.unlock();
    }
  }

  private void release(ContainerId container) {
    if (ref.remove(container)) {
      // 释放成功后更新最近引用时间戳
      timestamp.set(currentTime());
    } else {
      LOG.info("Container " + container
          + " doesn't exist in the container list of the Resource " + this
          + " to which it sent RELEASE event");
    }
  }

  private long currentTime() {
    return System.nanoTime();
  }

  /**
   * 获取资源当前状态。
   * @return 当前资源状态
   */
  public ResourceState getState() {
    this.readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 获取该资源对应的请求信息。
   * @return 资源请求对象
   */
  public LocalResourceRequest getRequest() {
    return rsrc;
  }

  /**
   * 获取资源本地化后的本地路径。
   * @return 本地文件路径
   */
  public Path getLocalPath() {
    return localPath;
  }

  /**
   * 设置资源本地化后的本地路径。
   * @param localPath 本地文件路径
   */
  public void setLocalPath(Path localPath) {
    this.localPath = Path.getPathWithoutSchemeAndAuthority(localPath);
  }

  /**
   * 获取资源最近被引用的时间戳。
   * @return 纳秒级时间戳
   */
  public long getTimestamp() {
    return timestamp.get();
  }

  /**
   * 获取资源大小。
   * @return 资源大小，单位字节
   */
  public long getSize() {
    return size;
  }

  /**
   * 获取当前使用该资源的容器数量（引用计数）。
   * @return 引用计数
   */
  public int getRefCount() {
    return ref.size();
  }

  /**
   * 尝试获取本地化锁，用于控制并发本地化。
   * @return 获取成功返回true
   */
  public boolean tryAcquire() {
    return sem.tryAcquire();
  }

  /**
   * 释放本地化锁。
   */
  public void unlock() {
    sem.release();
  }

  @Override
  public void handle(ResourceEvent event) {
    // 加写锁处理状态转移，保证线程安全
    this.writeLock.lock();
    try {
      Path resourcePath = event.getLocalResourceRequest().getPath();
      LOG.debug("Processing {} of type {}", resourcePath, event.getType());
      ResourceState oldState = this.stateMachine.getCurrentState();
      ResourceState newState = null;
      try {
        // 执行状态转移
        newState = this.stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error("Can't handle this event at current state", e);
      }
      // 状态变化打印日志
      if (newState != null && oldState != newState) {
        LOG.debug("Resource {}{} size : {} transitioned from {} to {}",
            resourcePath, (localPath != null ? "(->" + localPath + ")": ""),
            getSize(), oldState, newState);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 所有资源状态转移的抽象基类。
   */
  static abstract class ResourceTransition implements
      SingleArcTransition<LocalizedResource,ResourceEvent> {
    // typedef
  }

  /**
   * 从INIT到DOWNLOADING的状态转移处理，向本地化服务发送资源下载请求。
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  private static class FetchResourceTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      ResourceRequestEvent req = (ResourceRequestEvent) event;
      LocalizerContext ctxt = req.getContext();
      ContainerId container = ctxt.getContainerId();
      // 添加容器到引用列表
      rsrc.ref.add(container);
      // 发送下载请求给本地化服务
      rsrc.dispatcher.getEventHandler().handle(
          new LocalizerResourceRequestEvent(rsrc, req.getVisibility(), ctxt, 
              req.getLocalResourceRequest().getPattern()));
    }
  }

  /**
   * 本地化成功后的状态转移处理，通知所有等待该资源的容器。
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  private static class FetchSuccessTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      ResourceLocalizedEvent locEvent = (ResourceLocalizedEvent) event;
      // 保存本地化后的路径和大小
      rsrc.localPath =
          Path.getPathWithoutSchemeAndAuthority(locEvent.getLocation());
      rsrc.size = locEvent.getSize();
      // 通知所有等待的容器资源已就绪
      for (ContainerId container : rsrc.ref) {
        final ContainerResourceLocalizedEvent localizedEvent =
            new ContainerResourceLocalizedEvent(
                container, rsrc.rsrc, rsrc.localPath);
        localizedEvent.setSize(rsrc.size);
        rsrc.dispatcher.getEventHandler().handle(localizedEvent);
      }
    }
  }

  /**
   * 本地化失败后的状态转移处理，通知所有等待该资源的容器本地化失败。
   */
  @SuppressWarnings("unchecked")
  private static class FetchFailedTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      ResourceFailedLocalizationEvent failedEvent =
          (ResourceFailedLocalizationEvent) event;
      Queue<ContainerId> containers = rsrc.ref;
      // 通知所有等待容器本地化失败
      for (ContainerId container : containers) {
        rsrc.dispatcher.getEventHandler().handle(
          new ContainerResourceFailedEvent(container, failedEvent
            .getLocalResourceRequest(), failedEvent.getDiagnosticMessage()));
      }
    }
  }

  /**
   * 资源已本地化时处理新请求，直接通知请求容器资源已就绪。
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  private static class LocalizedResourceTransition
      extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      // 通知等待的容器，资源已就绪
      ResourceRequestEvent reqEvent = (ResourceRequestEvent) event;
      ContainerId container = reqEvent.getContext().getContainerId();
      // 添加容器到引用列表
      rsrc.ref.add(container);
      // 直接发送本地化完成事件
      final ContainerResourceLocalizedEvent localizedEvent =
          new ContainerResourceLocalizedEvent(
              container, rsrc.rsrc, rsrc.localPath);
      localizedEvent.setSize(-rsrc.size);
      rsrc.dispatcher.getEventHandler().handle(localizedEvent);
    }
  }

  /**
   * 处理容器释放资源请求，减少引用计数并更新时间戳。
   */
  private static class ReleaseTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      // 假设本地化中的容器最终都会成功或失败，释放一定会匹配请求
      ResourceReleaseEvent relEvent = (ResourceReleaseEvent) event;
      rsrc.release(relEvent.getContainer());
    }
  }

  /**
   * 恢复已本地化资源的状态转移处理，从NM重启恢复已有资源。
   */
  private static class RecoveredTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      ResourceRecoveredEvent recoveredEvent = (ResourceRecoveredEvent) event;
      // 恢复资源的本地路径和大小
      rsrc.localPath = recoveredEvent.getLocalPath();
      rsrc.size = recoveredEvent.getSize();
    }
  }
}