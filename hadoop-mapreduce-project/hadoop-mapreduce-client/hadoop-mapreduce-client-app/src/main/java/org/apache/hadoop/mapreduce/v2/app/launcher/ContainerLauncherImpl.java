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

package org.apache.hadoop.mapreduce.v2.app.launcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明: MapReduce ApplicationMaster 端容器启动器实现，负责向NodeManager发起容器启动/停止请求，
 * 处理任务容器的生命周期管理，维护容器状态，是AM和YARN NodeManager交互的核心组件。
 * This class is responsible for launching of containers.
 */
public class ContainerLauncherImpl extends AbstractService implements
    ContainerLauncher {

  static final Logger LOG =
      LoggerFactory.getLogger(ContainerLauncherImpl.class);

  /** 维护所有当前管理中的容器，key为容器ID */
  private ConcurrentHashMap<ContainerId, Container> containers = 
    new ConcurrentHashMap<ContainerId, Container>(); 
  private final AppContext context;
  protected ThreadPoolExecutor launcherPool;
  protected int initialPoolSize;
  private int limitOnPoolSize;
  private Thread eventHandlingThread;
  /** 容器启动事件队列，存放待处理的容器操作事件 */
  protected BlockingQueue<ContainerLauncherEvent> eventQueue =
      new LinkedBlockingQueue<ContainerLauncherEvent>();
  private final AtomicBoolean stopped;
  private ContainerManagementProtocolProxy cmProxy;

  /**
   * 根据事件获取容器信息，如果容器不存在则创建并加入缓存
   * @param event 容器操作事件
   * @return 对应容器对象
   */
  private Container getContainer(ContainerLauncherEvent event) {
    ContainerId id = event.getContainerID();
    Container c = containers.get(id);
    if(c == null) {
      c = new Container(event.getTaskAttemptID(), event.getContainerID(),
          event.getContainerMgrAddress());
      Container old = containers.putIfAbsent(id, c);
      if(old != null) {
        c = old;
      }
    }
    return c;
  }
  
  /**
   * 如果容器已经完全处理完成，从缓存中移除容器
   * @param id 容器ID
   */
  private void removeContainerIfDone(ContainerId id) {
    Container c = containers.get(id);
    if(c != null && c.isCompletelyDone()) {
      containers.remove(id);
    }
  }
  
  /**
   * 容器生命周期状态枚举
   */
  private enum ContainerState {
    /** 准备中 */
    PREP,
    /** 启动失败 */
    FAILED,
    /** 运行中 */
    RUNNING,
    /** 已完成 */
    DONE,
    /** 启动前被杀死 */
    KILLED_BEFORE_LAUNCH
  }

  /**
   * 容器内部信息维护类，保存单个容器的状态、所属任务尝试、位置等核心信息
   */
  private class Container {
    private ContainerState state;
    // store enough information to be able to cleanup the container
    private TaskAttemptId taskAttemptID;
    private ContainerId containerID;
    final private String containerMgrAddress;
    
    /**
     * 构造容器对象
     * @param taId 所属任务尝试ID
     * @param containerID 容器ID
     * @param containerMgrAddress 对应NodeManager地址
     */
    public Container(TaskAttemptId taId, ContainerId containerID,
        String containerMgrAddress) {
      this.state = ContainerState.PREP;
      this.taskAttemptID = taId;
      this.containerMgrAddress = containerMgrAddress;
      this.containerID = containerID;
    }
    
    /**
     * 检查容器是否已经完全处理完成
     * @return 是否完成
     */
    public synchronized boolean isCompletelyDone() {
      return state == ContainerState.DONE || state == ContainerState.FAILED;
    }

    /**
     * 将容器标记为已完成
     */
    public synchronized void done() {
      state = ContainerState.DONE;
    }

    @SuppressWarnings("unchecked")
    /**
     * 向远程NodeManager发起容器启动请求，处理启动结果处理
     * @param event 远程容器启动事件
     */
    public synchronized void launch(ContainerRemoteLaunchEvent event) {
      LOG.info("Launching " + taskAttemptID);
      if(this.state == ContainerState.KILLED_BEFORE_LAUNCH) {
        state = ContainerState.DONE;
        sendContainerLaunchFailedMsg(taskAttemptID, 
            "Container was killed before it was launched");
        return;
      }
      
      ContainerManagementProtocolProxyData proxy = null;
      try {
        // 获取对应NodeManager的通信代理
        proxy = getCMProxy(containerMgrAddress, containerID);

        // 构造容器启动上下文
        ContainerLaunchContext containerLaunchContext =
          event.getContainerLaunchContext();

        // 构造批量启动容器请求，这里只启动当前一个容器
        StartContainerRequest startRequest =
            StartContainerRequest.newInstance(containerLaunchContext,
              event.getContainerToken());
        List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
        list.add(startRequest);
        StartContainersRequest requestList = StartContainersRequest.newInstance(list);
        // 发起启动请求并获取响应
        StartContainersResponse response =
            proxy.getContainerManagementProtocol().startContainers(requestList);
        // 检查是否启动失败
        if (response.getFailedRequests() != null
            && response.getFailedRequests().containsKey(containerID)) {
          throw response.getFailedRequests().get(containerID).deSerialize();
        }
        // 从响应中提取Shuffle服务端口信息
        ByteBuffer portInfo =
            response.getAllServicesMetaData().get(
                ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID);
        int port = -1;
        if(portInfo != null) {
          port = ShuffleHandler.deserializeMetaData(portInfo);
        }
        LOG.info("Shuffle port returned by ContainerManager for "
            + taskAttemptID + " : " + port);

        if(port < 0) {
          this.state = ContainerState.FAILED;
          throw new IllegalStateException("Invalid shuffle port number "
              + port + " returned for " + taskAttemptID);
        }

        // 容器启动成功，发送容器启动完成事件，通知任务尝试切换为运行状态
        context.getEventHandler().handle(
            new TaskAttemptContainerLaunchedEvent(taskAttemptID, port));
        this.state = ContainerState.RUNNING;
      } catch (Throwable t) {
        String message = "Container launch failed for " + containerID + " : "
            + StringUtils.stringifyException(t);
        this.state = ContainerState.FAILED;
        // 发送启动失败事件，通知任务尝试处理失败
        sendContainerLaunchFailedMsg(taskAttemptID, message);
      } finally {
        if (proxy != null) {
          // 关闭或回收代理连接
          cmProxy.mayBeCloseProxy(proxy);
        }
      }
    }

    /**
     * 杀死容器，默认不输出线程栈
     */
    public void kill() {
      kill(false);
    }

    @SuppressWarnings("unchecked")
    /**
     * 向远程NodeManager发起容器停止请求，清理容器资源
     * @param dumpThreads 是否需要在停止前输出容器线程栈
     */
    public synchronized void kill(boolean dumpThreads) {

      if(this.state == ContainerState.PREP) {
        // 容器还未启动就被杀死，直接标记状态
        this.state = ContainerState.KILLED_BEFORE_LAUNCH;
      } else if (!isCompletelyDone()) {
        LOG.info("KILLING " + taskAttemptID);

        ContainerManagementProtocolProxyData proxy = null;
        try {
          // 获取NodeManager通信代理
          proxy = getCMProxy(this.containerMgrAddress, this.containerID);

          // 如果需要转储线程栈，先发送信号请求输出线程栈
          if (dumpThreads) {
            final SignalContainerRequest request = SignalContainerRequest
                .newInstance(containerID,
                    SignalContainerCommand.OUTPUT_THREAD_DUMP);
            proxy.getContainerManagementProtocol().signalToContainer(request);
          }

          // 构造停止容器请求
          List<ContainerId> ids = new ArrayList<ContainerId>();
          ids.add(this.containerID);
          StopContainersRequest request = StopContainersRequest.newInstance(ids);
          // 发起停止请求
          StopContainersResponse response =
              proxy.getContainerManagementProtocol().stopContainers(request);
          // 检查停止是否失败
          if (response.getFailedRequests() != null
              && response.getFailedRequests().containsKey(this.containerID)) {
            throw response.getFailedRequests().get(this.containerID)
              .deSerialize();
          }
        } catch (Throwable t) {
          // 清理失败仅记录日志，不影响整体流程
          String message = "cleanup failed for container "
              + this.containerID + " : "
              + StringUtils.stringifyException(t);
          context.getEventHandler()
              .handle(
                  new TaskAttemptDiagnosticsUpdateEvent(this.taskAttemptID,
                      message));
          LOG.warn(message);
        } finally {
          if (proxy != null) {
            cmProxy.mayBeCloseProxy(proxy);
          }
        }
        this.state = ContainerState.DONE;
      }
      // 发送容器清理完成事件，通知任务尝试处理结果
      context.getEventHandler().handle(
          new TaskAttemptEvent(this.taskAttemptID,
              TaskAttemptEventType.TA_CONTAINER_CLEANED));
    }
  }

  /**
   * 构造容器启动器实例
   * @param context ApplicationMaster上下文
   */
  public ContainerLauncherImpl(AppContext context) {
    super(ContainerLauncherImpl.class.getName());
    this.context = context;
    this.stopped = new AtomicBoolean(false);
  }

  @Override
  /**
   * 服务初始化方法，从配置中读取线程池参数，初始化YARN容器代理
   * @param conf 配置对象
   * @throws Exception 初始化异常
   */
  protected void serviceInit(Configuration conf) throws Exception {
    this.limitOnPoolSize = conf.getInt(
        MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT);
    LOG.info("Upper limit on the thread pool size is " + this.limitOnPoolSize);

    this.initialPoolSize = conf.getInt(
        MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE,
        MRJobConfig.DEFAULT_MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE);
    LOG.info("The thread pool initial size is " + this.initialPoolSize);

    super.serviceInit(conf);
    // 初始化容器管理协议代理，用于和NodeManager通信
    cmProxy = new ContainerManagementProtocolProxy(conf);
  }

  @Override
  /**
   * 服务启动方法，启动事件处理线程和容器启动线程池，开始处理容器操作事件
   * @throws Exception 启动异常
   */
  protected void serviceStart() throws Exception {

    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
        "ContainerLauncher #%d").setDaemon(true).build();

    // Start with a default core-pool size of 10 and change it dynamically.
    launcherPool = new HadoopThreadPoolExecutor(initialPoolSize,
        Integer.MAX_VALUE, 1, TimeUnit.HOURS,
        new LinkedBlockingQueue<Runnable>(),
        tf);
    // 初始化事件处理线程，负责从队列取出事件，动态调整线程池大小，提交给线程池处理
    eventHandlingThread = new SubjectInheritingThread() {
      @Override
      public void work() {
        ContainerLauncherEvent event = null;
        Set<String> allNodes = new HashSet<String>();

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            // 从事件队列阻塞取出待处理事件
            event = eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }
          // 记录当前需要交互的所有NodeManager节点
          allNodes.add(event.getContainerMgrAddress());

          int poolSize = launcherPool.getCorePoolSize();

          // 只有未达到线程池上限时才调整线程池大小
          if (poolSize != limitOnPoolSize) {

            // 当前需要交互的节点数，据此计算理想线程池大小
            int numNodes = allNodes.size();
            int idealPoolSize = Math.min(limitOnPoolSize, numNodes);

            if (poolSize < idealPoolSize) {
              // 增加缓冲预留容量，避免频繁调整
              int newPoolSize = Math.min(limitOnPoolSize, idealPoolSize
                  + initialPoolSize);
              LOG.info("Setting ContainerLauncher pool size to " + newPoolSize
                  + " as number-of-nodes to talk to is " + numNodes);
              // 更新核心线程池大小
              launcherPool.setCorePoolSize(newPoolSize);
            }
          }

          // 将事件处理任务提交给线程池并发处理
          launcherPool.execute(createEventProcessor(event));

          // TODO: Group launching of multiple containers to a single
          // NodeManager into a single connection
        }
      }
    };
    eventHandlingThread.setName("ContainerLauncher Event Handler");
    eventHandlingThread.start();
    super.serviceStart();
  }

  /**
   * 关闭所有还在运行中的容器，服务停止时调用
   */
  private void shutdownAllContainers() {
    for (Container ct : this.containers.values()) {
      if (ct != null) {
        ct.kill();
      }
    }
  }

  @Override
  /**
   * 服务停止方法，关闭事件线程、线程池，清理所有剩余容器
   * @throws Exception 停止异常
   */
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    // 停止所有还在运行的容器
    shutdownAllContainers();
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    if (launcherPool != null) {
      launcherPool.shutdownNow();
    }
    super.serviceStop();
  }

  /**
   * 创建事件处理器对象，子类可以重写扩展
   * @param event 待处理容器事件
   * @return 事件处理器Runnable对象
   */
  protected EventProcessor createEventProcessor(ContainerLauncherEvent event) {
    return new EventProcessor(event);
  }

  /**
   * 容器事件处理器，负责执行具体的容器启动/清理/完成操作
   * Setup and start the container on remote nodemanager.
   */
  class EventProcessor implements Runnable {