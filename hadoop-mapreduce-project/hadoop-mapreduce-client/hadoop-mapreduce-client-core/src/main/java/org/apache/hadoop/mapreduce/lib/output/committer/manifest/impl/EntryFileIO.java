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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.functional.FutureIO;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * 文件条目IO工具类，用于读写存放文件元数据条目的SequenceFile文件。
 * 支持同步读写，也支持异步写队列，允许多线程并发提交文件条目，由后台线程统一写入。
 * 条目文件本质是键值为{NullWritable, FileEntry}的SequenceFile。
 * 属于MapReduce输出提交器清单功能的底层IO工具，用于汇总多个任务生成的文件元数据。
 */
public class EntryFileIO {

  private static final Logger LOG = LoggerFactory.getLogger(
      EntryFileIO.class);

  /**
   * 写入器关闭超时时间，单位秒。
   */
  public static final int WRITER_SHUTDOWN_TIMEOUT_SECONDS = 60;

  /**
   * 写入队列入队超时时间，单位分钟。
   * 这是安全机制，避免队列异常时作业挂起不退出，超时后入队失败直接返回错误。
   */
  public static final int WRITER_QUEUE_PUT_TIMEOUT_MINUTES = 10;

  /** 用于加载文件系统的配置对象。 */
  private final Configuration conf;

  /**
   * 构造函数。
   * @param conf 用于加载文件系统的配置
   */
  public EntryFileIO(final Configuration conf) {
    this.conf = conf;
  }

  /**
   * 创建本地文件的SequenceFile写入器。
   * @param file 本地文件路径
   * @return SequenceFile写入器
   * @throws IOException 创建文件失败时抛出
   */
  public SequenceFile.Writer createWriter(File file) throws IOException {
    return createWriter(toPath(file));
  }

  /**
   * 创建任意文件系统上文件的SequenceFile写入器。
   * @param path 写入目标路径
   * @return SequenceFile写入器
   * @throws IOException 创建文件失败时抛出
   */
  public SequenceFile.Writer createWriter(Path path) throws IOException {
    return SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(path),
        SequenceFile.Writer.keyClass(NullWritable.class),
        SequenceFile.Writer.valueClass(FileEntry.class));
  }


  /**
   * 创建本地文件的SequenceFile读取器，用于顺序读取。
   * @param file 本地文件路径
   * @return SequenceFile读取器
   * @throws IOException 打开文件失败时抛出
   */
  public SequenceFile.Reader createReader(File file) throws IOException {
    return createReader(toPath(file));
  }

  /**
   * 创建任意文件系统上文件的SequenceFile读取器，用于顺序读取。
   * @param path 读取目标路径
   * @return SequenceFile读取器
   * @throws IOException 打开文件失败时抛出
   */
  public SequenceFile.Reader createReader(Path path) throws IOException {
    return new SequenceFile.Reader(conf,
        SequenceFile.Reader.file(path));
  }

  /**
   * 创建文件条目迭代器，从SequenceFile中顺序读取文件条目。
   * 需要强转关闭，关闭迭代器会同时关闭底层读取器。
   * @param reader SequenceFile读取器
   * @return 文件条目远程迭代器
   */
  public RemoteIterator<FileEntry> iterateOver(SequenceFile.Reader reader) {
    return new EntryIterator(reader);
  }

  /**
   * 创建并启动异步条目写入器，后台线程处理队列写入。
   * @param writer 底层SequenceFile写入器
   * @param capacity 队列容量
   * @return 启动完成的异步写入器
   */
  public EntryWriter launchEntryWriter(SequenceFile.Writer writer, int capacity) {
    final EntryWriter ew = new EntryWriter(writer, capacity);
    ew.start();
    return ew;
  }

  /**
   * 同步批量写入文件条目到写入器，可选择写入后关闭流。
   * @param writer SequenceFile写入器
   * @param entries 待写入的文件条目集合
   * @param close 是否写入完成后关闭流
   * @return 写入的条目数量
   * @throws IOException 写入失败时抛出
   */
  public static int write(SequenceFile.Writer writer,
      Collection<FileEntry> entries,
      boolean close)
      throws IOException {
    try {
      for (FileEntry entry : entries) {
        writer.append(NullWritable.get(), entry);
      }
      writer.flush();
    } finally {
      if (close) {
        writer.close();
      }
    }
    return entries.size();
  }


  /**
   * 将Java本地File对象转换为Hadoop Path对象。
   * @param file Java本地文件对象
   * @return Hadoop Path对象
   */
  public static Path toPath(final File file) {
    return new Path(file.toURI());
  }


  /**
   * 写入队列支持的操作类型枚举。
   */
  private enum Actions {
    /** 写入一批条目。 */
    write,
    /** 停止处理线程。 */
    stop
  }

  /**
   * 队列中的元素，包含操作类型和待写入条目列表。
   */
  private static final class QueueEntry {

    private final Actions action;

    private final List<FileEntry> entries;

    private QueueEntry(final Actions action, List<FileEntry> entries) {
      this.action = action;
      this.entries = entries;
    }

    private QueueEntry(final Actions action) {
      this(action, null);
    }
  }

  /**
   * 异步文件条目写入器，使用阻塞队列实现多线程生产-单线程消费写入。
   * 多个线程可并发提交文件条目列表，后台线程统一序列化写入SequenceFile。
   * 队列满时生产者会阻塞，超时后入队失败；支持安全关闭，等待所有队列条目写入完成后关闭流。
   * 核心设计用于汇总多个MapReduce任务生成的文件元数据，生成最终的清单文件。
   */
  public static final class EntryWriter implements Closeable {

    /** 底层SequenceFile写入器，负责实际写入。 */
    private final SequenceFile.Writer writer;

    /** 阻塞队列，存放待处理的操作和条目。 */
    private final BlockingQueue<QueueEntry> queue;

    /** 停止标志，通知处理线程退出循环。 */
    private final AtomicBoolean stop = new AtomicBoolean(false);

    /** 处理线程是否处于活跃状态。 */
    private final AtomicBoolean active = new AtomicBoolean(false);

    private final int capacity;

    /** 执行后台处理线程的线程池。 */
    private ExecutorService executor;

    /** 后台处理任务的Future对象。 */
    private Future<Integer> future;

    /** 已写入条目计数器，仅在后台线程更新，使用原子变量保证可见性。 */
    private final AtomicInteger count = new AtomicInteger();

    /** 保存后台线程写入过程中捕获的IO异常，供主线程检查抛出。 */
    private final AtomicReference<IOException> failure = new AtomicReference<>();

    /**
     * 构造异步写入器。
     * @param writer 底层SequenceFile写入器
     * @param capacity 队列容量
     */
    private EntryWriter(SequenceFile.Writer writer, int capacity) {
      checkState(capacity > 0, "invalid queue capacity %s", capacity);
      this.writer = requireNonNull(writer);
      this.capacity = capacity;
      this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * 检查写入器是否活跃。
     * @return 处理线程存活时返回true
     */
    public boolean isActive() {
      return active.get();
    }

    /**
     * 获取已写入的条目总数。
     * @return 已写入条目数
     */
    public int getCount() {
      return count.get();
    }

    /**
     * 获取写入过程中发生的异常。
     * @return 如果发生写入异常返回异常对象，否则返回null
     */
    public IOException getFailure() {
      return failure.get();
    }

    /**
     * 启动后台处理线程。
     */
    private void start() {
      checkState(executor == null, "already started");
      active.set(true);
      executor = HadoopExecutors.newSingleThreadExecutor();
      future = executor.submit(this::processor);
      LOG.debug("Started entry writer {}", this);
    }

    /**
     * 将一批文件条目加入写入队列。
     * @param entries 待写入的条目列表
     * @return 入队成功返回true，失败返回false
     */
    public boolean enqueue(List<FileEntry> entries) {
      if (entries.isEmpty()) {
        LOG.debug("ignoring enqueue of empty list");
        // 空列表快速返回，仍然返回成功
        return true;
      }
      if (active.get()) {
        try {
          LOG.debug("Queueing {} entries", entries.size());
          // 超时入队，避免队列异常挂起
          final boolean enqueued = queue.offer(new QueueEntry(Actions.write, entries),
              WRITER_QUEUE_PUT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
          if (!enqueued) {
            LOG.warn("Timeout submitting entries to {}", this);
          }
          return enqueued;
        } catch (InterruptedException e) {
          Thread.interrupted();
          return false;
        }
      } else {
        LOG.warn("EntryFile write queue inactive; discarding {} entries submitted to {}",
            entries.size(), this);
        return false;
      }
    }

    /**
     * 后台处理循环，从队列取出操作并执行，直到收到停止指令。
     * @return 处理完成的总条目数
     * @throws UncheckedIOException 写入失败时抛出未检查IO异常
     */
    private int processor() {
      Thread.currentThread().setName("EntryIOWriter");
      try {
        while (!stop.get()) {
          final QueueEntry queueEntry = queue.take();
          switch (queueEntry.action) {

          case stop:  // 停止处理
            LOG.debug("Stop processing");
            stop.set(true);
            break;

          case write:  // 写入一批条目
          default:  // 兼容编译器检查
            // 写入所有条目
            final List<FileEntry> entries = queueEntry.entries;
            LOG.debug("Adding block of {} entries", entries.size());
            for (FileEntry entry : entries) {
              append(entry);
            }
            break;
          }
        }
      } catch (IOException e) {
        LOG.debug("Write failure", e);
        failure.set(e);
        throw new UncheckedIOException(e);
      } catch (InterruptedException e) {
        // 隐式停止
        LOG.debug("interrupted", e);
      } finally {
        stop.set(true);
        active.set(false);
        // 清空队列，唤醒所有阻塞线程
        queue.clear();
      }
      return count.get();
    }

    /**
     * 写入单个文件条目。
     * @param entry 待写入条目
     * @throws IOException 写入失败时抛出
     */
    private void append(FileEntry entry) throws IOException {
      writer.append(NullWritable.get(), entry);

      final int c = count.incrementAndGet();
      LOG.trace("Added entry #{}: {}", c, entry);
    }

    /**
     * 关闭写入器：停止接受新入队，等待所有队列条目写入完成后关闭流。
     * @throws IOException 关闭过程中发生IO异常或写入失败时抛出
     */
    @Override
    public void close() throws IOException {

      // 标记为不活跃，停止接受新入队
      if (!active.getAndSet(false)) {
        // 已经停止，直接返回
        return;
      }
      LOG.debug("Shutting down writer; entry lists in queue: {}",
          capacity - queue.remainingCapacity());

      // 入队停止操作，会在所有现有条目处理完后执行
      try {
        queue.put(new QueueEntry(Actions.stop));
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
      try {
        // 等待处理完成，超时则强制关闭
        int total = FutureIO.awaitFuture(future, WRITER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        LOG.debug("Processed {} files", total);
        executor.shutdown();
      } catch (TimeoutException e) {
        LOG.warn("Timeout waiting for write thread to finish");
        // 超时强制关闭线程池
        executor.shutdownNow();
      } finally {
        // 无论如何都关闭底层写入流
        writer.close();
      }
    }

    /**
     * 如果后台线程写入过程发生过异常，在此抛出该异常。
     * @throws IOException 保存了异常时抛出
     */
    public void maybeRaiseWriteException() throws IOException {
      final IOException f = failure.get();
      if (f != null) {
        throw f;
      }
    }

    @Override
    public String toString() {
      return "EntryWriter{" +
          "stop=" + stop.get() +
          ", active=" + active.get() +
          ", count=" + count.get() +
          ", queue depth=" + queue.size() +
          ", failure=" + failure +
          '}';
    }
  }


  /**
   * 文件条目迭代器，从SequenceFile中顺序读取文件条目。
   * 读取到文件末尾后自动关闭底层读取器，非线程安全，仅支持单线程遍历。
   */
  @VisibleForTesting
  static final class EntryIterator implements RemoteIterator<FileEntry>, Closeable {

    private final SequenceFile.Reader reader;

    private FileEntry fetched;

    private boolean closed;

    private int count;

    /**
     * 构造迭代器。
     * @param reader 底层SequenceFile读取器
     */
    private EntryIterator(final SequenceFile.Reader reader) {
      this.reader = requireNonNull(reader);
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        reader.close();
      }
    }

    @Override
    public String toString() {
      return "EntryIterator{" +
          "closed=" + closed +
          ", count=" + count +
          ", fetched=" + fetched +
          '}';
    }

    @Override
    public boolean hasNext() throws IOException {
      return fetched != null || fetchNext();
    }

    /**
     * 预读取下一个条目，读取失败或到末尾时自动关闭读取器。
     * @return 成功读取到条目返回true，到文件末尾返回false
     * @throws IOE 读取过程发生IO异常时抛出
     */
    private boolean fetchNext() throws IOException {
      FileEntry readBack = new FileEntry();
      if (reader.next(NullWritable.get(), readBack)) {
        fetched = readBack;
        count++;
        return true;
      } else {
        fetched = null;
        close();
        return false;
      }
    }

    @Override
    public FileEntry next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final FileEntry r = fetched;
      fetched = null;
      return r;
    }

    /**
     * 检查迭代器是否已关闭。
     * @return 已关闭返回true
     */
    public boolean isClosed() {
      return closed;
    }

    int getCount() {
      return count;
    }
  }

}