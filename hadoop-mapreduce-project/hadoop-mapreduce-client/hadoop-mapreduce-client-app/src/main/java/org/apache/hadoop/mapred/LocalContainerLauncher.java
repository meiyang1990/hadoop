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

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.hadoop.service.AbstractService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：本地化容器启动器，用于在uber模式下将MapReduce任务容器在当前MRAppMaster JVM内启动执行
 * 核心职责：在本地同一JVM中顺序执行Map/Reduce子任务，避免多任务同时操作同一本地目录导致文件冲突
 */
/**
 * Runs the container task locally in a thread.
 * Since all (sub)tasks share the same local directory, they must be executed
 * sequentially in order to avoid creating/deleting the same files/dirs.
 */
public class LocalContainerLauncher extends AbstractService implements
    ContainerLauncher {

  private static final File curDir = new File(".");
  private static final Logger LOG =
      LoggerFactory.getLogger(LocalContainerLauncher.class);

  private FileContext curFC = null;
  private Set<File> localizedFiles = new HashSet<File>();
  private final AppContext context;
  private final TaskUmbilicalProtocol umbilical;
  private final ClassLoader jobClassLoader;
  private ExecutorService taskRunner;
  private Thread eventHandler;
  private byte[] encryptedSpillKey = new byte[] {0};
  private BlockingQueue<ContainerLauncherEvent> eventQueue =
      new LinkedBlockingQueue<ContainerLauncherEvent>();

  /**
   * 构造函数，不指定自定义类加载器
   * @param context MR应用上下文
   * @param umbilical 任务通信协议
   */
  public LocalContainerLauncher(AppContext context,
                                TaskUmbilicalProtocol umbilical) {
    this(context, umbilical, null);
  }

  /**
   * 构造函数，支持自定义作业类加载器
   * @param context MR应用上下文
   * @param umbilical 任务通信协议
   * @param jobClassLoader 作业自定义类加载器
   */
  public LocalContainerLauncher(AppContext context,
                                TaskUmbilicalProtocol umbilical,
                                ClassLoader jobClassLoader) {
    super(LocalContainerLauncher.class.getName());
    this.context = context;
    this.umbilical = umbilical;
        // umbilical:  MRAppMaster creates (taskAttemptListener), passes to us
        // (TODO/FIXME:  pointless to use RPC to talk to self; should create
        // LocalTaskAttemptListener or similar:  implement umbilical protocol
        // but skip RPC stuff)
    this.jobClassLoader = jobClassLoader;

    try {
      curFC = FileContext.getFileContext(curDir.toURI());
    } catch (UnsupportedFileSystemException ufse) {
      LOG.error("Local filesystem " + curDir.toURI().toString()
                + " is unsupported?? (should never happen)");
    }

    // Save list of files/dirs that are supposed to be present so can delete
    // any extras created by one task before starting subsequent task.  Note
    // that there's no protection against deleted or renamed localization;
    // users who do that get what they deserve (and will have to disable
    // uberization in order to run correctly).
    File[] curLocalFiles = curDir.listFiles();
    if (curLocalFiles != null) {
      HashSet<File> lf = new HashSet<File>(curLocalFiles.length);
      for (int j = 0; j < curLocalFiles.length; ++j) {
        lf.add(curLocalFiles[j]);
      }
      localizedFiles = Collections.unmodifiableSet(lf);
    }

    // Relocalization note/future FIXME (per chrisdo, 20110315):  At moment,
    // full localization info is in AppSubmissionContext passed from client to
    // RM and then to NM for AM-container launch:  no difference between AM-
    // localization and MapTask- or ReduceTask-localization, so can assume all
    // OK.  Longer-term, will need to override uber-AM container-localization
    // request ("needed resources") with union of regular-AM-resources + task-
    // resources (and, if maps and reduces ever differ, then union of all three
    // types), OR will need localizer service/API that uber-AM can request
    // after running (e.g., "localizeForTask()" or "localizeForMapTask()").
  }

  @Override
  public void serviceStart() throws Exception {
    // 创建单线程线程池，保证任务顺序执行
    taskRunner =
        HadoopExecutors.newSingleThreadExecutor(new ThreadFactoryBuilder().
            setDaemon(true).setNameFormat("uber-SubtaskRunner").build());
    // 创建并启动事件处理线程
    eventHandler = new SubjectInheritingThread(new EventHandler(), "uber-EventHandler");
    // 如果指定了作业类加载器，设置为事件处理线程的上下文类加载器
    if (jobClassLoader != null) {
      LOG.info("Setting " + jobClassLoader +
          " as the context classloader of thread " + eventHandler.getName());
      eventHandler.setContextClassLoader(jobClassLoader);
    } else {
      // 记录当前上下文类加载器
      LOG.info("Context classloader of thread " + eventHandler.getName() +
          ": " + eventHandler.getContextClassLoader());
    }
    eventHandler.start();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    if (eventHandler != null) {
      eventHandler.interrupt();
    }
    if (taskRunner != null) {
      taskRunner.shutdownNow();
    }
    super.serviceStop();
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);  // FIXME? YarnRuntimeException is "for runtime exceptions only"
    }
  }

  /**
   * 设置加密溢写密钥，用于加密中间溢写文件
   * @param encryptedSpillKey 加密密钥
   */
  public void setEncryptedSpillKey(byte[] encryptedSpillKey) {
    if (encryptedSpillKey != null) {
      this.encryptedSpillKey = encryptedSpillKey;
    }
  }

  /*
   * Uber-AM lifecycle/ordering ("normal" case):
   *
   * - [somebody] sends TA_ASSIGNED
   *   - handled by ContainerAssignedTransition (TaskAttemptImpl.java)
   *     - creates "remoteTask" for us == real Task
   *     - sends CONTAINER_REMOTE_LAUNCH
   *     - TA: UNASSIGNED -> ASSIGNED
   * - CONTAINER_REMOTE_LAUNCH handled by LocalContainerLauncher (us)
   *   - sucks "remoteTask" out of TaskAttemptImpl via getRemoteTask()
   *   - sends TA_CONTAINER_LAUNCHED
   *     [[ elsewhere...
   *       - TA_CONTAINER_LAUNCHED handled by LaunchedContainerTransition
   *         - registers "remoteTask" with TaskAttemptListener (== umbilical)
   *         - NUKES "remoteTask"
   *         - sends T_ATTEMPT_LAUNCHED (Task: SCHEDULED -> RUNNING)
   *         - TA: ASSIGNED -> RUNNING
   *     ]]
   *   - runs Task (runSubMap() or runSubReduce())
   *     - TA can safely send TA_UPDATE since in RUNNING state
   */
  /**
   * 事件处理器类，负责处理容器启动/清理事件，顺序执行子任务
   */
  private class EventHandler implements Runnable {

    // doneWithMaps and finishedSubMaps are accessed from only
    // one thread. Therefore, no need to make them volatile.
    private boolean doneWithMaps = false;
    private int finishedSubMaps = 0;

    private final Map<TaskAttemptId,Future<?>> futures =
        new ConcurrentHashMap<TaskAttemptId,Future<?>>();

    EventHandler() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      ContainerLauncherEvent event = null;

      // 保存Map任务输出位置，供Reduce任务本地读取
      final Map<TaskAttemptID, MapOutputFile> localMapFiles =
          new HashMap<TaskAttemptID, MapOutputFile>();
      
      // _must_ either run subtasks sequentially or accept expense of new JVMs
      // (i.e., fork()), else will get weird failures when maps try to create/
      // write same dirname or filename:  no chdir() in Java
      while (!Thread.currentThread().isInterrupted()) {
        try {
          // 从事件队列取出事件
          event = eventQueue.take();
        } catch (InterruptedException e) {  // mostly via T_KILL? JOB_KILL?
          LOG.warn("Returning, interrupted : " + e);
          break;
        }

        LOG.info("Processing the event " + event.toString());

        if (event.getType() == EventType.CONTAINER_REMOTE_LAUNCH) {

          final ContainerRemoteLaunchEvent launchEv =
              (ContainerRemoteLaunchEvent)event;
          
          // 提交任务到线程池执行
          Future<?> future = taskRunner.submit(new Runnable() {
            public void run() {
              runTask(launchEv, localMapFiles);
            }
          });
          // 保存任务Future，后续清理使用
          futures.put(event.getTaskAttemptID(), future);

        } else if (event.getType() == EventType.CONTAINER_REMOTE_CLEANUP) {

          // 如果需要，转储当前所有线程栈
          if (event.getDumpContainerThreads()) {
            try {
              // 构造线程转储头部信息
              System.out.println(new java.util.Date());
              RuntimeMXBean rtBean = ManagementFactory.getRuntimeMXBean();
              System.out.println("Full thread dump " + rtBean.getVmName()
                  + " (" + rtBean.getVmVersion()
                  + " " + rtBean.getSystemProperties().get("java.vm.info")
                  + "):\n");
              // 转储所有线程状态和栈信息
              ThreadMXBean tmxBean = ManagementFactory.getThreadMXBean();
              ThreadInfo[] tInfos = tmxBean.dumpAllThreads(
                  tmxBean.isObjectMonitorUsageSupported(),
                  tmxBean.isSynchronizerUsageSupported());
              for (ThreadInfo ti : tInfos) {
                System.out.println(ti.toString());
              }
            } catch (Throwable t) {
              // 线程转储失败不影响主流程
              System.out.println("Could not create full thread dump: "
                  + t.getMessage());
            }
          }

          // 取消并中断对应任务尝试
          TaskAttemptId taId = event.getTaskAttemptID();
          Future<?> future = futures.remove(taId);
          if (future != null) {
            LOG.info("canceling the task attempt " + taId);
            future.cancel(true);
          }

          // 发送容器清理完成事件，推进任务尝试状态机
          context.getEventHandler().handle(
              new TaskAttemptEvent(taId,
                  TaskAttemptEventType.TA_CONTAINER_CLEANED));
        } else if (event.getType() == EventType.CONTAINER_COMPLETED) {
          LOG.debug("Container completed " + event.toString());
        } else {
          LOG.warn("Ignoring unexpected event " + event.toString());
        }

      }
    }

    @SuppressWarnings("unchecked")
    /**
     * 执行容器启动任务，封装任务执行的完整流程
     * @param launchEv 容器远程启动事件
     * @param localMapFiles 保存Map任务输出的本地路径
     */
    private void runTask(ContainerRemoteLaunchEvent launchEv,
        Map<TaskAttemptID, MapOutputFile> localMapFiles) {
      TaskAttemptId attemptID = launchEv.getTaskAttemptID(); 

      Job job = context.getAllJobs().get(attemptID.getTaskId().getJobId());
      int numMapTasks = job.getTotalMaps();
      int numReduceTasks = job.getTotalReduces();

      // YARN框架层任务对象
      org.apache.hadoop.mapreduce.v2.app.job.Task ytask =
          job.getTask(attemptID.getTaskId());
      // 经典MapReduce任务对象
      org.apache.hadoop.mapred.Task remoteTask = launchEv.getRemoteTask();

      // 启动完成后发送已启动事件，推进任务尝试从ASSIGNED到RUNNING状态
      // 本地模式没有远程端口，shuffle直接读本地文件，因此端口设置为-1
      
      //There is no port number because we are not really talking to a task
      // tracker.  The shuffle is just done through local files.  So the
      // port number is set to -1 in this case.
      context.getEventHandler().handle(
          new TaskAttemptContainerLaunchedEvent(attemptID, -1));

      // 如果没有Map任务，直接标记Map阶段完成
      if (numMapTasks == 0) {
        doneWithMaps = true;
      }

      try {
        if (remoteTask.isMapOrReduce()) {
          // 更新作业计数器：uber任务总启动数+1
          JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.TOTAL_LAUNCHED_UBERTASKS, 1);
          if (remoteTask.isMapTask()) {
            jce.addCounterUpdate(JobCounter.NUM_UBER_SUBMAPS, 1);
          } else {
            jce.addCounterUpdate(JobCounter.NUM_UBER_SUBREDUCES, 1);
          }
          context.getEventHandler().handle(jce);
        }
        // 执行子任务
        runSubtask(remoteTask, ytask.getType(), attemptID, numMapTasks,
                   (numReduceTasks > 0), localMapFiles);

        // 非uber模式由NM->RM->AM通知容器完成，uber模式在当前JVM执行，需要模拟发送完成事件
        context.getEventHandler().handle(new TaskAttemptEvent(attemptID,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));

      } catch (RuntimeException re) {
        // 更新作业计数器：uber任务失败数+1
        JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID.getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.NUM_FAILED_UBERTASKS, 1);
        context.getEventHandler().handle(jce);
        // 子任务执行失败，模拟容器完成事件推进状态机到失败状态