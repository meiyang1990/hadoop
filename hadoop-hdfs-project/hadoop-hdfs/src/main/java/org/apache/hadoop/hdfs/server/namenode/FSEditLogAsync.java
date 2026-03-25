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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 异步实现的HDFS编辑日志管理器，通过后台线程异步落盘编辑日志，降低NameNode前台RPC延迟
 * 继承自FSEditLog，将日志写入和同步操作解耦到后台线程执行，提升NameNode响应性能
 */
class FSEditLogAsync extends FSEditLog implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(FSEditLog.class);

  // 保护同步线程状态的独立互斥锁，避免停止线程时出现死锁
  private final Object syncThreadLock = new Object();
  private Thread syncThread;
  // 线程本地变量存储当前线程待同步的编辑日志实例
  private static final ThreadLocal<Edit> THREAD_EDIT = new ThreadLocal<Edit>();

  // 等待后台线程处理的 pending 编辑日志队列，多线程并发访问需要线程安全
  private final BlockingQueue<Edit> editPendingQ;

  // 已写入日志但等待同步完成的编辑日志队列，仅由后台同步线程访问，无需同步
  // 队列无界，大小受编辑日志缓冲区限制，最终会强制触发同步
  private final Deque<Edit> syncWaitQ = new ArrayDeque<Edit>();

  // 上次记录队列满日志的时间戳
  private long lastFull = 0;

  /**
   * 构造异步编辑日志管理器，初始化pending队列并禁用操作实例缓存
   * @param conf 配置对象
   * @param storage NameNode存储管理器
   * @param editsDirs 编辑日志存储目录列表
   */
  FSEditLogAsync(Configuration conf, NNStorage storage, List<URI> editsDirs) {
    super(conf, storage, editsDirs);
    // 由于操作实例会被后台线程消费，无法复用缓存，因此禁用缓存
    cache.disableCache();
    int editPendingQSize = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING_PENDING_QUEUE_SIZE,
        DFSConfigKeys.
            DFS_NAMENODE_EDITS_ASYNC_LOGGING_PENDING_QUEUE_SIZE_DEFAULT);

    editPendingQ = new ArrayBlockingQueue<>(editPendingQSize);
  }

  /**
   * 检查后台同步线程是否存活
   * @return 线程存活返回true，否则返回false
   */
  private boolean isSyncThreadAlive() {
    synchronized(syncThreadLock) {
      return syncThread != null && syncThread.isAlive();
    }
  }

  /**
   * 启动后台编辑日志同步线程，如果线程已退出则重新启动
   */
  private void startSyncThread() {
    synchronized(syncThreadLock) {
      if (!isSyncThreadAlive()) {
        syncThread = new SubjectInheritingThread(this, this.getClass().getSimpleName());
        syncThread.start();
      }
    }
  }

  /**
   * 停止后台编辑日志同步线程，中断并等待线程退出
   */
  private void stopSyncThread() {
    synchronized(syncThreadLock) {
      if (syncThread != null) {
        try {
          syncThread.interrupt();
          syncThread.join();
        } catch (InterruptedException e) {
          // 进程即将退出，忽略中断异常
        } finally {
          syncThread = null;
        }
      }
    }
  }

  @VisibleForTesting
  @Override
  public void restart() {
    stopSyncThread();
    startSyncThread();
  }

  @Override
  void openForWrite(int layoutVersion) throws IOException {
    try {
      startSyncThread();
      super.openForWrite(layoutVersion);
    } catch (IOException ioe) {
      stopSyncThread();
      throw ioe;
    }
  }

  @Override
  public void close() {
    super.close();
    stopSyncThread();
  }

  @Override
  void logEdit(final FSEditLogOp op) {
    assert isOpenForWrite();

    Edit edit = getEditInstance(op);
    THREAD_EDIT.set(edit);
    synchronized(this) {
      enqueueEdit(edit);
      beginTransaction(op);
    }
  }

  @Override
  public void logSync() {
    Edit edit = THREAD_EDIT.get();
    if (edit != null) {
      // 不删除ThreadLocal条目，避免rehash和内存清理开销
      THREAD_EDIT.set(null);
      if (LOG.isDebugEnabled()) {
        LOG.debug("logSync " + edit);
      }
      // 等待异步同步完成
      edit.logSyncWait();
    }
  }

  @Override
  public void logSyncAll() {
    // 该方法本身不写入日志，仅保证返回时所有已入队日志都完成同步
    // 构造特殊同步编辑日志，触发全量队列刷新
    Edit edit = new SyncEdit(this, null){
      @Override
      public boolean logEdit() {
        return true;
      }
    };
    enqueueEdit(edit);
    edit.logSyncWait();
  }

  // 排空信号量用于实现高优先级预留，避免队列满时线程饥饿
  // 逻辑存在少量竞态但满足业务需求足够
  private Semaphore overflowMutex = new Semaphore(8){
    private AtomicBoolean draining = new AtomicBoolean();
    private AtomicInteger pendingReleases = new AtomicInteger();
    @Override
    public int drainPermits() {
      draining.set(true);
      return super.drainPermits();
    }
    // 排空过程中暂存需要释放的许可数量，排空完成后统一释放
    private void tryRelease(int permits) {
      pendingReleases.getAndAdd(permits);
      if (!draining.get()) {
        super.release(pendingReleases.getAndSet(0));
      }
    }
    @Override
    public void release() {
      tryRelease(1);
    }
    @Override
    public void release(int permits) {
      draining.set(false);
      tryRelease(permits);
    }
  };

  /**
   * 将编辑日志实例入队到pending队列，处理队列满时的流量控制
   * @param edit 待处理编辑日志实例
   */
  private void enqueueEdit(Edit edit) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("logEdit " + edit);
    }
    try {
      // 优先非阻塞入队，避免对正常流程产生性能开销，持续溢出时才进行限流
      if (!editPendingQ.offer(edit)) {
        Preconditions.checkState(
            isSyncThreadAlive(), "sync thread is not alive");
        long now = Time.monotonicNow();
        // 每4秒最多打印一次队列满日志，避免日志刷屏
        if (now - lastFull > 4000) {
          lastFull = now;
          LOG.info("Edit pending queue is full");
        }
        if (Thread.holdsLock(this)) {
          // 调用方已经持有编辑日志锁，必须先释放锁等待，避免与后台线程死锁
          // 后台线程写日志也需要持有锁，所以这里必须释放锁让渡给后台
          int permits = overflowMutex.drainPermits();
          try {
            do {
              this.wait(1000); // 等待队列有空间后重试
            } while (!editPendingQ.offer(edit));
          } finally {
            overflowMutex.release(permits);
          }
        } else {
          // 调用方未持有锁，通过信号量限流，降低并发排队竞争
          overflowMutex.acquire();
          try {
            editPendingQ.put(edit);
          } finally {
            overflowMutex.release();
          }
        }
      }
    } catch (Throwable t) {
      // 入队失败属于致命错误，直接终止NameNode
      terminate(t);
    }
  }

  /**
   * 从pending队列取出编辑日志，用于后台线程处理
   * @return 取出的编辑日志实例，超时/非阻塞返回null
   * @throws InterruptedException 线程中断时抛出
   */
  private Edit dequeueEdit() throws InterruptedException {
    // 如果已有待同步的日志，非阻塞取出，先完成同步再处理新日志
    return syncWaitQ.isEmpty() ? editPendingQ.take() : editPendingQ.poll();
  }

  /**
   * 后台同步线程主运行方法，持续从队列取出编辑日志写入并同步
   */
  @Override
  public void run() {
    try {
      while (true) {
        NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
        boolean doSync;
        Edit edit = dequeueEdit();
        if (edit != null) {
          // 写入日志，判断是否需要同步
          doSync = edit.logEdit();
          syncWaitQ.add(edit);
          metrics.setPendingEditsCount(editPendingQ.size() + 1);
        } else {
          // pending队列为空，但仍有待同步的日志，触发同步
          doSync = !syncWaitQ.isEmpty();
          metrics.setPendingEditsCount(0);
        }
        if (doSync) {
          // 正常编辑日志同步异常会终止NameNode，此处捕获用于测试可见
          RuntimeException syncEx = null;
          try {
            logSync(getLastWrittenTxId());
          } catch (RuntimeException ex) {
            syncEx = ex;
          }
          // 通知所有等待同步的编辑日志，同步已完成
          while ((edit = syncWaitQ.poll()) != null) {
            edit.logSyncNotify(syncEx);
          }
        }
      }
    } catch (InterruptedException ie) {
      LOG.info(Thread.currentThread().getName() + " was interrupted, exiting");
    } catch (Throwable t) {
      terminate(t);
    }
  }

  /**
   * 处理异步日志过程中的致命错误，终止NameNode进程
   * @param t 抛出的异常
   */
  private void terminate(Throwable t) {
    String message = "Exception while edit logging: "+t.getMessage();
    LOG.error(message, t);
    ExitUtil.terminate(1, message);
  }

  /**
   * 根据调用上下文创建对应类型的编辑日志实例
   * @param op 编辑日志操作
   * @return 编辑日志包装实例（RPC异步调用返回RpcEdit，同步调用返回SyncEdit）
   */
  private Edit getEditInstance(FSEditLogOp op) {
    final Edit edit;
    final Server.Call rpcCall = Server.getCurCall().get();
    // 仅未持有日志锁的RPC调用才会走异步化处理
    if (rpcCall != null && !Thread.holdsLock(this)) {
      edit = new RpcEdit(this, op, rpcCall);
    } else {
      edit = new SyncEdit(this, op);
    }
    return edit;
  }

  /**
   * 抽象编辑日志包装类，定义异步处理生命周期方法
   */
  private abstract static class Edit {
    final FSEditLog log;
    final FSEditLogOp op;

    Edit(FSEditLog log, FSEditLogOp op) {
      this.log = log;
      this.op = op;
    }

    /**
     * 后台线程执行日志写入，返回是否需要同步落盘
     * @return 需要同步返回true，否则返回false
     */
    boolean logEdit() {
      return log.doEditTransaction(op);
    }

    /**
     * 等待后台同步完成的抽象方法
     */
    abstract void logSyncWait();
    /**
     * 同步完成后通知等待线程的抽象方法
     * @param ex 同步过程中抛出的异常，成功则为null
     */
    abstract void logSyncNotify(RuntimeException ex);
  }

  /**
   * 同步等待编辑日志实现，调用线程需要阻塞等待同步完成
   */
  private static class SyncEdit extends Edit {
    private final Object lock;
    private boolean done = false;
    private RuntimeException syncEx;

    SyncEdit(FSEditLog log, FSEditLogOp op) {
      super(log, op);
      // 如果当前线程已持有日志锁（例如日志滚动场景），则使用日志锁作为等待锁
      // 避免死锁，否则使用当前对象锁，减少对主日志锁的竞争
      lock = Thread.holdsLock(log) ? log : this;
    }

    @Override
    public void logSyncWait() {
      synchronized(lock) {
        while (!done) {
          try {
            lock.wait(10);
          } catch (InterruptedException e) {}
        }
        // 仅测试场景需要，正常情况下异常会直接终止NameNode
        if (syncEx != null) {
          syncEx.fillInStackTrace();
          throw syncEx;
        }
      }
    }

    @Override
    public void logSyncNotify(RuntimeException ex) {
      synchronized(lock) {
        done = true;
        syncEx = ex;
        lock.notifyAll();
      }
    }

    @Override
    public String toString() {
      return "["+getClass().getSimpleName()+" op:"+op+"]";
    }
  }

  /**
   * RPC异步编辑日志实现，RPC调用线程可提前返回，延迟发送响应直到同步完成
   */
  private static class RpcEdit extends Edit {
    private final Server.Call call;

    RpcEdit(FSEditLog log, FSEditLogOp op, Server.Call call) {
      super(log, op);
      this.call = call;
      // 推迟发送RPC响应，直到同步完成
      call.postponeResponse();
    }

    @Override
    public void logSyncWait() {
      // 空操作，让RPC线程立刻释放，提升吞吐量，响应延迟到同步完成后发送
    }

    @Override
    public void logSyncNotify(RuntimeException syncEx) {
      try {
        if (syncEx == null) {
          // 同步成功，发送RPC响应给客户端
          call.sendResponse();
        } else {
          // 同步失败，响应异常给客户端
          call.abortResponse(syncEx);
        }
      } catch (Exception e) {} // 发送失败不处理，不影响NameNode主流程
    }

    @Override
    public String toString() {
      return "["+getClass().getSimpleName()+" op:"+op+" call:"+call+"]";
    }
  }
}