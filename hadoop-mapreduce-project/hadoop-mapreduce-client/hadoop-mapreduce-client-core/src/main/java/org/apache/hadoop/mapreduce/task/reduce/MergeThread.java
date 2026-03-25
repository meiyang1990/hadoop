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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce Shuffle阶段归并线程抽象基类，负责从待处理队列中取出归并任务执行归并操作
 * 为不同类型的归并（内存归并、磁盘归并）提供统一的线程调度框架
 * @param <T> 归并输入段类型
 * @param <K> 输出键类型
 * @param <V> 输出值类型
 */
abstract class MergeThread<T,K,V> extends SubjectInheritingThread {
  
  private static final Logger LOG = LoggerFactory.getLogger(MergeThread.class);

  // 待处理归并任务计数
  private AtomicInteger numPending = new AtomicInteger(0);
  // 待归并输入段队列，每个元素是一次归并的输入段列表
  private LinkedList<List<T>> pendingToBeMerged;
  // 归并管理器引用，用于和主流程交互
  protected final MergeManagerImpl<K,V> manager;
  // 异常报告器，用于将异常上报给作业任务
  private final ExceptionReporter reporter;
  // 线程是否已关闭标记
  private boolean closed = false;
  // 归并因子，每次归并最多处理的分段数
  private final int mergeFactor;
  
  /**
   * 构造归并线程实例
   * @param manager 归并管理器实例
   * @param mergeFactor 归并因子，每次归并最多处理的分段数
   * @param reporter 异常上报器
   */
  public MergeThread(MergeManagerImpl<K,V> manager, int mergeFactor,
                     ExceptionReporter reporter) {
    this.pendingToBeMerged = new LinkedList<List<T>>();
    this.manager = manager;
    this.mergeFactor = mergeFactor;
    this.reporter = reporter;
  }
  
  /**
   * 关闭归并线程，等待现有归并任务完成后中断线程
   * @throws InterruptedException 等待过程中被中断抛出
   */
  public synchronized void close() throws InterruptedException {
    closed = true;
    waitForMerge();
    interrupt();
  }

  /**
   * 启动一次新的归并，从输入集合中取出最多归并因子数量的分段，添加到待归并队列
   * @param inputs 待归并分段集合
   */
  public void startMerge(Set<T> inputs) {
    if (!closed) {
      // 增加待处理任务计数
      numPending.incrementAndGet();
      List<T> toMergeInputs = new ArrayList<T>();
      Iterator<T> iter=inputs.iterator();
      // 取出最多mergeFactor个分段进行本次归并
      for (int ctr = 0; iter.hasNext() && ctr < mergeFactor; ++ctr) {
        toMergeInputs.add(iter.next());
        iter.remove();
      }
      LOG.info(getName() + ": Starting merge with " + toMergeInputs.size() + 
               " segments, while ignoring " + inputs.size() + " segments");
      synchronized(pendingToBeMerged) {
        // 添加到待归并队列尾部
        pendingToBeMerged.addLast(toMergeInputs);
        // 唤醒等待任务的归并线程
        pendingToBeMerged.notifyAll();
      }
    }
  }

  /**
   * 等待所有待处理归并任务完成
   * @throws InterruptedException 等待过程中被中断抛出
   */
  public synchronized void waitForMerge() throws InterruptedException {
    while (numPending.get() > 0) {
      wait();
    }
  }

  /**
   * 归并线程主工作循环，不断从待队列取出任务执行归并
   */
  public void work() {
    while (true) {
      List<T> inputs = null;
      try {
        // 等待归并任务通知
        synchronized (pendingToBeMerged) {
          while(pendingToBeMerged.size() <= 0) {
            pendingToBeMerged.wait();
          }
          // 取出队首的归并任务输入
          inputs = pendingToBeMerged.removeFirst();
        }

        // 执行归并，由子类实现具体逻辑
        merge(inputs);
      } catch (InterruptedException ie) {
        // 被中断，清空待处理计数后退出
        numPending.set(0);
        return;
      } catch(Throwable t) {
        // 发生异常，清空待处理计数，上报异常后退出
        numPending.set(0);
        reporter.reportException(t);
        return;
      } finally {
        // 减少待处理计数，通知等待者任务完成
        synchronized (this) {
          numPending.decrementAndGet();
          notifyAll();
        }
      }
    }
  }

  /**
   * 抽象归并方法，由具体子类实现不同类型的归并逻辑
   * @param inputs 待归并的输入分段列表
   * @throws IOException 归并过程中IO异常抛出
   */
  public abstract void merge(List<T> inputs) throws IOException;

  @VisibleForTesting
  int getMergeFactor() {
    return mergeFactor;
  }

  @VisibleForTesting
  LinkedList<List<T>> getPendingToBeMerged() {
    return pendingToBeMerged;
  }
}