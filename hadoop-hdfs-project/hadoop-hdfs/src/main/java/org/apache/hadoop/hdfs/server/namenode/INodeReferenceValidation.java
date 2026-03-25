// 这个文件已经全部加上中文注释
/*
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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.FsImageValidation.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hdfs.server.namenode.FsImageValidation.Cli.*;

/**
 * INodeReference子类引用验证工具，用于FsImage校验过程中检查INode引用的正确性。
 * 在HDFS快照等场景中会使用INodeReference共享INode，该工具负责验证所有引用的完整性和一致性。
 */
public class INodeReferenceValidation {
  public static final Logger LOG = LoggerFactory.getLogger(
      INodeReferenceValidation.class);

  // 单例实例原子引用，保证线程安全的单例创建与销毁
  private static final AtomicReference<INodeReferenceValidation> INSTANCE
      = new AtomicReference<>();

  /**
   * 启动INode引用验证流程，创建单例验证实例。
   * 仅当实例不存在时才会创建，重复调用不会重复创建。
   */
  public static void start() {
    INSTANCE.compareAndSet(null, new INodeReferenceValidation());
    println("%s started", INodeReferenceValidation.class.getSimpleName());
  }

  /**
   * 结束INode引用验证流程，执行引用检查并统计错误数，销毁单例实例。
   * @param errorCount 错误计数器，用于累加验证过程中发现的错误数量
   */
  public static void end(AtomicInteger errorCount) {
    final INodeReferenceValidation instance = INSTANCE.getAndSet(null);
    if (instance == null) {
      return;
    }

    final int initCount = errorCount.get();
    instance.assertReferences(errorCount);
    println("%s ended successfully: %d error(s) found.",
        INodeReferenceValidation.class.getSimpleName(),
        errorCount.get() - initCount);
  }

  /**
   * 添加一个INodeReference到对应类型的验证集合中。
   * 仅当验证已启动时才会添加，验证未启动时直接忽略。
   * @param ref 待验证的INodeReference对象
   * @param clazz INodeReference的具体类型
   * @param <REF> 泛型，约束类型与对象一致
   */
  static <REF extends INodeReference> void add(REF ref, Class<REF> clazz) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      final boolean added = validation.getReferences(clazz).add(ref);
      Preconditions.checkState(added);
      LOG.trace("add {}: {}", clazz, ref.toDetailString());
    }
  }

  /**
   * 从验证集合中移除一个INodeReference。
   * 仅当验证已启动时才会移除，验证未启动时直接忽略。
   * @param ref 待移除的INodeReference对象
   * @param clazz INodeReference的具体类型
   * @param <REF> 泛型，约束类型与对象一致
   */
  static <REF extends INodeReference> void remove(REF ref, Class<REF> clazz) {
    final INodeReferenceValidation validation = INSTANCE.get();
    if (validation != null) {
      final boolean removed = validation.getReferences(clazz).remove(ref);
      Preconditions.checkState(removed);
      LOG.trace("remove {}: {}", clazz, ref.toDetailString());
    }
  }

  /**
   * 按类型分类存储INode引用的集合，支持并行验证任务的提交与进度跟踪。
   * @param <REF> 存储的INodeReference具体类型
   */
  static class ReferenceSet<REF extends INodeReference> {
    private final Class<REF> clazz;
    // 存储待验证的INode引用列表
    private final List<REF> references = new LinkedList<>();
    // 已拆分的验证任务列表
    private volatile List<Task<REF>> tasks;
    // 异步任务的Future结果列表
    private volatile List<Future<Integer>> futures;
    // 已完成的任务计数，用于进度统计
    private final AtomicInteger taskCompleted = new AtomicInteger();

    ReferenceSet(Class<REF> clazz) {
      this.clazz = clazz;
    }

    boolean add(REF ref) {
      return references.add(ref);
    }

    boolean remove(REF ref) {
      for(final Iterator<REF> i = references.iterator(); i.hasNext();) {
        if (i.next() == ref) {
          i.remove();
          return true;
        }
      }
      return false;
    }

    /**
     * 提交当前集合中所有待验证引用到线程池，拆分生成验证任务并行执行。
     * @param errorCount 错误计数器
     * @param service 并行任务执行线程池
     * @throws InterruptedException 线程中断异常
     */
    void submit(AtomicInteger errorCount, ExecutorService service)
        throws InterruptedException {
      final int size = references.size();
      tasks = createTasks(references, errorCount);
      println("Submitting %d tasks for validating %s %s(s)",
          tasks.size(), Util.toCommaSeparatedNumber(size),
          clazz.getSimpleName());
      futures = service.invokeAll(tasks);
    }

    /**
     * 等待所有验证任务完成，收集任务结果。
     * @throws Exception 任务执行异常
     */
    void waitForFutures() throws Exception {
      for(Future<Integer> f : futures) {
        f.get();
        taskCompleted.incrementAndGet();
      }
    }

    /**
     * 计算当前已完成任务的百分比，用于进度日志输出。
     * @return 完成百分比（0-100）
     */
    double getTaskCompletedPercent() {
      final List<Task<REF>> t = tasks;
      return t == null? 0
          : t.isEmpty()? 100
          : taskCompleted.get()*100.0/tasks.size();
    }

    @Override
    public String toString() {
      return String.format("%s %.1f%%",
          clazz.getSimpleName(), getTaskCompletedPercent());
    }
  }

  // 存储WithCount类型引用集合
  private final ReferenceSet<INodeReference.WithCount> withCounts
      = new ReferenceSet<>(INodeReference.WithCount.class);
  // 存储WithName类型引用集合
  private final ReferenceSet<INodeReference.WithName> withNames
      = new ReferenceSet<>(INodeReference.WithName.class);
  // 存储DstReference类型引用集合
  private final ReferenceSet<INodeReference.DstReference> dstReferences
      = new ReferenceSet<>(INodeReference.DstReference.class);

  /**
   * 根据INodeReference类型获取对应的存储集合。
   * @param clazz INodeReference类型
   * @param <REF> 泛型类型
   * @return 对应类型的引用集合
   */
  <REF extends INodeReference> ReferenceSet<REF> getReferences(
      Class<REF> clazz) {
    if (clazz == INodeReference.WithCount.class) {
      return (ReferenceSet<REF>) withCounts;
    } else if (clazz == INodeReference.WithName.class) {
      return (ReferenceSet<REF>) withNames;
    } else if (clazz == INodeReference.DstReference.class) {
      return (ReferenceSet<REF>) dstReferences;
    }
    throw new IllegalArgumentException("References not found for " + clazz);
  }

  /**
   * 执行所有INode引用的验证，利用多线程并行处理提升验证速度。
   * 定时输出验证进度日志，验证完成后释放资源。
   * @param errorCount 错误计数器，用于累加验证发现的错误
   */
  private void assertReferences(AtomicInteger errorCount) {
    // 获取可用CPU核心数，创建对应大小的线程池
    final int p = Runtime.getRuntime().availableProcessors();
    LOG.info("Available Processors: {}", p);
    final ExecutorService service = Executors.newFixedThreadPool(p);

    // 定时任务：每秒输出一次各类型引用的验证进度
    final TimerTask checkProgress = new TimerTask() {
      @Override
      public void run() {
        LOG.info("ASSERT_REFERENCES Progress: {}, {}, {}",
            dstReferences, withCounts, withNames);
      }
    };
    final Timer t = new Timer();
    // 启动定时进度输出，间隔1秒
    t.scheduleAtFixedRate(checkProgress, 0, 1_000);

    try {
      // 提交三类引用的验证任务
      dstReferences.submit(errorCount, service);
      withCounts.submit(errorCount, service);
      withNames.submit(errorCount, service);

      // 等待所有任务完成
      dstReferences.waitForFutures();
      withCounts.waitForFutures();
      withNames.waitForFutures();
    } catch (Throwable e) {
      printError("Failed to assertReferences", e);
    } finally {
      // 关闭线程池和定时器，释放资源
      service.shutdown();
      t.cancel();
    }
  }

  /**
   * 将待验证引用列表拆分为多个批量验证任务，每个任务处理固定大小的引用。
   * @param references 待验证引用列表
   * @param errorCount 错误计数器
   * @param <REF> 泛型类型
   * @return 拆分后的验证任务列表
   */
  static <REF extends INodeReference> List<Task<REF>> createTasks(
      List<REF> references, AtomicInteger errorCount) {
    final List<Task<REF>> tasks = new LinkedList<>();
    for (final Iterator<REF> i = references.iterator(); i.hasNext();) {
      tasks.add(new Task<>(i, errorCount));
    }
    return tasks;
  }

  /**
   * 批量INode引用验证任务，实现Callable接口支持并行执行。
   * 每个任务固定处理BATCH_SIZE个引用，控制任务粒度。
   * @param <REF> 待验证的INodeReference类型
   */
  static class Task<REF extends INodeReference> implements Callable<Integer> {
    // 每个任务批量处理的引用数量
    static final int BATCH_SIZE = 100_000;

    // 当前任务需要处理的引用列表
    private final List<REF> references = new LinkedList<>();
    // 全局错误计数器
    private final AtomicInteger errorCount;

    /**
     * 构造任务，从迭代器中取出BATCH_SIZE个引用作为当前任务的处理对象。
     * @param i 待验证引用迭代器
     * @param errorCount 全局错误计数器
     */
    Task(Iterator<REF> i, AtomicInteger errorCount) {
      for(int n = 0; i.hasNext() && n < BATCH_SIZE; n++) {
        references.add(i.next());
        i.remove();
      }
      this.errorCount = errorCount;
    }

    /**
     * 执行当前任务的批量验证，逐个调用引用自身的assertReference方法检查。
     * 捕获所有验证异常，统计错误数不中断整体验证流程。
     * @return 当前任务处理的引用数量
     * @throws Exception 任务执行异常
     */
    @Override
    public Integer call() throws Exception {
      for (final REF ref : references) {
        try {
          ref.assertReferences();
        } catch (Throwable t) {
          printError(errorCount, "%s", t);
        }
      }
      return references.size();
    }
  }
}