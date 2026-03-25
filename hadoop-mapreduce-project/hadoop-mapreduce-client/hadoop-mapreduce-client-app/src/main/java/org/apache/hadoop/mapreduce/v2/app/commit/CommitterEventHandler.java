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

package org.apache.hadoop.mapreduce.v2.app.commit;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobAbortCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCommitCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCommitFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobSetupCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobSetupFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 输出提交器事件处理器，负责处理MapReduce作业生命周期中与输出提交相关的各类事件
 * 包括作业初始化、作业提交、作业终止、任务终止等操作，异步处理事件避免阻塞主流程
 */
public class CommitterEventHandler extends AbstractService
    implements EventHandler<CommitterEvent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommitterEventHandler.class);

  private final AppContext context;
  private final OutputCommitter committer;
  private final RMHeartbeatHandler rmHeartbeatHandler;
  private ThreadPoolExecutor launcherPool;
  private Thread eventHandlingThread;
  private BlockingQueue<CommitterEvent> eventQueue =
      new LinkedBlockingQueue<CommitterEvent>();
  private final AtomicBoolean stopped;
  private final ClassLoader jobClassLoader;
  private Thread jobCommitThread = null;
  private int commitThreadCancelTimeoutMs;
  private long commitWindowMs;
  private FileSystem fs;
  private Path startCommitFile;
  private Path endCommitSuccessFile;
  private Path endCommitFailureFile;
  

  /**
   * 构造提交器事件处理器
   * @param context ApplicationMaster上下文
   * @param committer 输出提交器实例
   * @param rmHeartbeatHandler ResourceManager心跳处理器
   */
  public CommitterEventHandler(AppContext context, OutputCommitter committer,
      RMHeartbeatHandler rmHeartbeatHandler) {
    this(context, committer, rmHeartbeatHandler, null);
  }
  
  /**
   * 构造提交器事件处理器，指定作业类加载器
   * @param context ApplicationMaster上下文
   * @param committer 输出提交器实例
   * @param rmHeartbeatHandler ResourceManager心跳处理器
   * @param jobClassLoader 作业自定义类加载器
   */
  public CommitterEventHandler(AppContext context, OutputCommitter committer,
      RMHeartbeatHandler rmHeartbeatHandler, ClassLoader jobClassLoader) {
    super("CommitterEventHandler");
    this.context = context;
    this.committer = committer;
    this.rmHeartbeatHandler = rmHeartbeatHandler;
    this.stopped = new AtomicBoolean(false);
    this.jobClassLoader = jobClassLoader;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 读取提交线程取消超时配置
    commitThreadCancelTimeoutMs = conf.getInt(
        MRJobConfig.MR_AM_COMMITTER_CANCEL_TIMEOUT_MS,
        MRJobConfig.DEFAULT_MR_AM_COMMITTER_CANCEL_TIMEOUT_MS);
    // 读取有效提交窗口配置
    commitWindowMs = conf.getLong(MRJobConfig.MR_AM_COMMIT_WINDOW_MS,
        MRJobConfig.DEFAULT_MR_AM_COMMIT_WINDOW_MS);
    try {
      // 获取文件系统实例
      fs = FileSystem.get(conf);
      // 构造作业ID并生成提交状态文件路径
      JobID id = TypeConverter.fromYarn(context.getApplicationID());
      JobId jobId = TypeConverter.toYarn(id);
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      startCommitFile = MRApps.getStartJobCommitFile(conf, user, jobId);
      endCommitSuccessFile = MRApps.getEndJobCommitSuccessFile(conf, user, jobId);
      endCommitFailureFile = MRApps.getEndJobCommitFailureFile(conf, user, jobId);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    // 构建线程工厂，命名处理线程
    ThreadFactoryBuilder tfBuilder = new ThreadFactoryBuilder()
        .setNameFormat("CommitterEvent Processor #%d");
    if (jobClassLoader != null) {
      // 如果启用了作业类加载器，需要将其设置为处理线程的上下文类加载器
      // 保证提交器可以通过TCCL加载作业自定义类
      ThreadFactory backingTf = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = new SubjectInheritingThread(r);
          thread.setContextClassLoader(jobClassLoader);
          return thread;
        }
      };
      tfBuilder.setThreadFactory(backingTf);
    }
    ThreadFactory tf = tfBuilder.build();
    // 初始化固定大小线程池处理事件
    launcherPool = new HadoopThreadPoolExecutor(5, 5, 1,
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>(), tf);
    // 创建事件拉取线程，从队列取出事件交给线程池处理
    eventHandlingThread = new SubjectInheritingThread(new Runnable() {
      @Override
      public void run() {
        CommitterEvent event = null;
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }
          // 使用线程池并行处理队列中的事件
          launcherPool.execute(new EventProcessor(event));        }
      }
    });
    eventHandlingThread.setName("CommitterEvent Handler");
    eventHandlingThread.start();
    super.serviceStart();
  }


  @Override
  /**
   * 将提交事件放入事件队列等待处理
   * @param event 待处理的提交事件
   */
  public void handle(CommitterEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // 已停止则直接返回
      return;
    }
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    if (launcherPool != null) {
      launcherPool.shutdown();
    }
    super.serviceStop();
  }

  /**
   * 标记作业提交开始，保证同一时间只有一个提交线程运行
   * @throws IOException 如果已有提交线程正在运行则抛出异常
   */
  private synchronized void jobCommitStarted() throws IOException {
    if (jobCommitThread != null) {
      throw new IOException("Commit while another commit thread active: "
          + jobCommitThread.toString());
    }

    jobCommitThread = Thread.currentThread();
  }

  /**
   * 标记作业提交结束，清空当前提交线程并唤醒等待线程
   */
  private synchronized void jobCommitEnded() {
    if (jobCommitThread == Thread.currentThread()) {
      jobCommitThread = null;
      notifyAll();
    }
  }

  /**
   * 取消正在进行的作业提交，中断提交线程并等待其终止
   */
  private synchronized void cancelJobCommit() {
    Thread threadCommitting = jobCommitThread;
    if (threadCommitting != null && threadCommitting.isAlive()) {
      LOG.info("Cancelling commit");
      threadCommitting.interrupt();

      // 等待提交线程在超时时间内完成退出
      long now = context.getClock().getTime();
      long timeoutTimestamp = now + commitThreadCancelTimeoutMs;
      try {
        while (jobCommitThread == threadCommitting
            && now > timeoutTimestamp) {
          wait(now - timeoutTimestamp);
          now = context.getClock().getTime();
        }
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * 事件处理器，负责处理单个提交器事件并路由到对应处理方法
   */
  private class EventProcessor implements Runnable {
    private CommitterEvent event;

    /**
     * 构造事件处理器实例
     * @param event 需要处理的提交事件
     */
    EventProcessor(CommitterEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());
      // 根据事件类型分发到对应处理方法
      switch (event.getType()) {
      case JOB_SETUP:
        handleJobSetup((CommitterJobSetupEvent) event);
        break;
      case JOB_COMMIT:
        handleJobCommit((CommitterJobCommitEvent) event);
        break;
      case JOB_ABORT:
        handleJobAbort((CommitterJobAbortEvent) event);
        break;
      case TASK_ABORT:
        handleTaskAbort((CommitterTaskAbortEvent) event);
        break;
      default:
        throw new YarnRuntimeException("Unexpected committer event "
            + event.toString());
      }
    }
    
    /**
     * 处理作业初始化事件，调用OutputCommitter完成作业setup并发送完成事件
     * @param event 作业初始化事件
     */
    @SuppressWarnings("unchecked")
    protected void handleJobSetup(CommitterJobSetupEvent event) {
      try {
        committer.setupJob(event.getJobContext());
        context.getEventHandler().handle(
            new JobSetupCompletedEvent(event.getJobID()));
      } catch (Exception e) {
        LOG.warn("Job setup failed", e);
        context.getEventHandler().handle(new JobSetupFailedEvent(
            event.getJobID(), StringUtils.stringifyException(e)));
      }
    }

    /**
     * 创建空文件标记提交状态，支持可重复提交时覆盖已有文件
     * @param p 要创建的文件路径
     * @param overwrite 是否允许覆盖已有文件
     * @throws IOException 创建文件失败时抛出
     */
    // If job commit is repeatable, then we should allow
    // startCommitFile/endCommitSuccessFile/endCommitFailureFile to be written
    // by other AM before.
    private void touchz(Path p, boolean overwrite) throws IOException {
      fs.create(p, overwrite).close();
    }

    /**
     * 处理作业提交事件，调用OutputCommitter完成作业输出提交并处理结果
     * @param event 作业提交事件
     */
    @SuppressWarnings("unchecked")
    protected void handleJobCommit(CommitterJobCommitEvent event) {
      boolean commitJobIsRepeatable = false;
      try {
        // 检查提交器是否支持可重复提交
        commitJobIsRepeatable = committer.isCommitJobRepeatable(
            event.getJobContext());
      } catch (IOException e) {
        LOG.warn("Exception in committer.isCommitJobRepeatable():", e);
      }

      try {
        // 创建提交开始标记文件
        touchz(startCommitFile, commitJobIsRepeatable);
        // 标记提交开始
        jobCommitStarted();
        // 等待到有效提交窗口再执行提交，保证心跳正常
        waitForValidCommitWindow();
        // 执行作业提交
        committer.commitJob(event.getJobContext());
        // 创建提交成功标记文件
        touchz(endCommitSuccessFile, commitJobIsRepeatable);
        // 发送提交完成事件
        context.getEventHandler().handle(
            new JobCommitCompletedEvent(event.getJobID()));
      } catch (Exception e) {
        LOG.error("Could not commit job", e);
        try {
          // 创建提交失败标记文件
          touchz(endCommitFailureFile, commitJobIsRepeatable);
        } catch (Exception e2) {
          LOG.error("could not create failure file.", e2);
        }
        // 发送提交失败事件
        context.getEventHandler().handle(
            new JobCommitFailedEvent(event.getJobID(),
                StringUtils.stringifyException(e)));
      } finally {
        // 标记提交结束
        jobCommitEnded();
      }
    }

    /**
     * 处理作业终止事件，取消正在进行的提交，调用OutputCommitter终止作业并发送完成事件
     * @param event 作业终止事件
     */
    @SuppressWarnings("unchecked")
    protected void handleJobAbort(CommitterJobAbortEvent event) {
      // 先取消正在进行的提交
      cancelJobCommit();

      try {
        committer.abortJob(event.getJobContext(), event.getFinalState());
      } catch (Exception e) {
        LOG.warn("Could not abort job", e);
      }

      context.getEventHandler().handle(new JobAbortCompletedEvent(
          event.getJobID(), event.getFinalState()));
    }

    /**
     * 处理任务终止事件，调用OutputCommitter清理任务输出并发送清理完成事件
     * @param event 任务终止事件
     */
    @SuppressWarnings("unchecked")
    protected void handleTaskAbort(CommitterTaskAbortEvent event) {
      try {
        committer.abortTask(event.getAttemptContext());
      } catch (Exception e) {
        LOG.warn("Task cleanup failed for attempt " + event.getAttemptID(), e);
      }
      context.getEventHandler().handle(
          new TaskAttemptEvent(event.getAttemptID(),
              TaskAttemptEventType.TA_CLEANUP_DONE));
    }

    /**
     * 等待有效提交窗口：距离上次RM心跳在指定时间范围内才允许提交
     * 避免在RM长时间心跳超时后提交，提升提交可靠性
     * @throws InterruptedException 线程等待被中断时抛出
     */
    private synchronized void waitForValidCommitWindow()
        throws InterruptedException {
      long lastHeartbeatTime = rmHeartbeatHandler.getLastHeartbeatTime();
      long now = context.getClock().getTime();

      // 如果距离上次心跳超过窗口大小，等待下一次心跳后再继续
      while (now - lastHeartbeatTime > commitWindowMs) {
        rmHeartbeatHandler.runOnNextHeartbeat(new Runnable() {
          @Override
          public void run() {
            synchronized (EventProcessor.this) {
              EventProcessor.this.notify();
            }
          }
        });

        wait();
        lastHeartbeatTime = rmHeartbeatHandler.getLastHeartbeatTime();
        now = context.getClock().getTime();
      }
    }
  }
}