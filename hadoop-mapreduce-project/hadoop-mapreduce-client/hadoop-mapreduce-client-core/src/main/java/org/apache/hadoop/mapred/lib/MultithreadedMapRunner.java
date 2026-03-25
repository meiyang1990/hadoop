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

package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * 文件说明：MapRunnable接口的多线程实现类
 * <p>
 * 当Map操作不是CPU密集型任务时，可以使用该实现代替默认的MapRunner实现，提升Map阶段整体吞吐量
 * <p>
 * 使用该MapRunnable的用户自定义Mapper必须是线程安全的
 * <p>
 * 使用方式需要通过JobConf.setMapRunnerClass方法配置该类，
 * 并且可通过<code>mapred.map.multithreadedrunner.threads</code>配置线程池大小，默认值为10
 * <p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultithreadedMapRunner<K1, V1, K2, V2>
    implements MapRunnable<K1, V1, K2, V2> {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultithreadedMapRunner.class.getName());

  private JobConf job;
  private Mapper<K1, V1, K2, V2> mapper;
  private ExecutorService executorService;
  private volatile IOException ioException;
  private volatile RuntimeException runtimeException;
  private boolean incrProcCount;

  /**
   * 配置多线程MapRunner，初始化线程池和Mapper实例
   * @param jobConf 作业配置对象
   */
  @SuppressWarnings("unchecked")
  public void configure(JobConf jobConf) {
    // 从配置读取线程数，默认10
    int numberOfThreads =
      jobConf.getInt(MultithreadedMapper.NUM_THREADS, 10);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configuring jobConf " + jobConf.getJobName() +
                " to use " + numberOfThreads + " threads");
    }

    this.job = jobConf;
    // 仅在跳过坏记录功能开启时，才递增处理记录计数器
    this.incrProcCount = SkipBadRecords.getMapperMaxSkipRecords(jobConf)>0 && 
      SkipBadRecords.getAutoIncrMapperProcCount(jobConf);
    // 通过反射实例化用户自定义Mapper
    this.mapper = ReflectionUtils.newInstance(jobConf.getMapperClass(),
        jobConf);

    // 创建固定大小线程池，用于并行执行Mapper的map方法
    executorService = new HadoopThreadPoolExecutor(numberOfThreads,
        numberOfThreads,
                                             0L, TimeUnit.MILLISECONDS,
                                             new BlockingArrayQueue
                                               (numberOfThreads));
  }

  /**
   * 自定义阻塞队列，修改默认offer/add操作行为：当队列满时阻塞等待，而非直接抛出异常
   */
  private static class BlockingArrayQueue extends ArrayBlockingQueue<Runnable> {
 
    private static final long serialVersionUID = 1L;
    public BlockingArrayQueue(int capacity) {
      super(capacity);
    }
    public boolean offer(Runnable r) {
      return add(r);
    }
    public boolean add(Runnable r) {
      try {
        put(r);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      return true;
    }
  }

  /**
   * 检查工作线程是否抛出异常，如果存在异常，在主线程重新抛出，保持默认实现的错误语义
   * @throws IOException 工作线程抛出的IO异常
   * @throws RuntimeException 工作线程抛出的运行时异常
   */
  private void checkForExceptionsFromProcessingThreads()
      throws IOException, RuntimeException {
    if (ioException != null) {
      throw ioException;
    }

    if (runtimeException != null) {
      throw runtimeException;
    }
  }

  /**
   * 执行Map阶段，多线程并行处理输入记录
   * @param input 输入记录读取器
   * @param output 输出收集器
   * @param reporter 作业报告器
   * @throws IOException 读取输入或处理记录时IO异常
   */
  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
                  Reporter reporter)
    throws IOException {
    try {
      // 每个键值对创建新实例，因为并发执行无法复用对象
      K1 key = input.createKey();
      V1 value = input.createValue();

      while (input.next(key, value)) {
        // 提交map任务到线程池
        executorService.execute(new MapperInvokeRunable(key, value, output,
                                reporter));
        // 检查是否已有工作线程抛出异常
        checkForExceptionsFromProcessingThreads();
        // 创建新的键值对实例，供下一个任务使用
        key = input.createKey();
        value = input.createValue();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished dispatching all Mappper.map calls, job "
                  + job.getJobName());
      }
      // 关闭线程池，不再接受新任务，等待已提交任务执行完成
      executorService.shutdown();

      try {
        // 等待所有任务执行完成，每100ms检查一次
        while (!executorService.awaitTermination(100, TimeUnit.MILLISECONDS)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Awaiting all running Mappper.map calls to finish, job "
                      + job.getJobName());
          }
          // 等待过程中仍检查是否有异常抛出
          checkForExceptionsFromProcessingThreads();
        }
        // 终止后仍需检查异常，处理刚好在终止后发生异常的边界情况
        checkForExceptionsFromProcessingThreads();

      } catch (IOException ioEx) {
        // 强制中断所有线程，重新抛出异常
        executorService.shutdownNow();
        throw ioEx;
      } catch (InterruptedException iEx) {
        throw new RuntimeException(iEx);
      }

    } finally {
      // 关闭Mapper实例
      mapper.close();
    }
  }


  /**
   * 单个map任务执行单元，封装单个输入键值对的map调用，在线程池中执行
   */
  private class MapperInvokeRunable implements Runnable {
    private K1 key;
    private V1 value;
    private OutputCollector<K2, V2> output;
    private Reporter reporter;

    /**
     * 构造单个map任务执行单元
     * @param key 输入键
     * @param value 输入值
     * @param output 输出收集器
     * @param reporter 作业报告器
     */
    public MapperInvokeRunable(K1 key, V1 value,
                               OutputCollector<K2, V2> output,
                               Reporter reporter) {
      this.key = key;
      this.value = value;
      this.output = output;
      this.reporter = reporter;
    }

    /**
     * 执行单个map调用，捕获异常并保存到外层对象
     */
    public void run() {
      try {
        // 调用Mapper处理当前键值对
        MultithreadedMapRunner.this.mapper.map(key, value, output, reporter);
        // 如果开启坏记录跳过功能，递增已处理记录计数器
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
        }
      } catch (IOException ex) {
        // 保存IO异常，供主线程检查重抛
        synchronized (MultithreadedMapRunner.this) {
          if (MultithreadedMapRunner.this.ioException == null) {
            MultithreadedMapRunner.this.ioException = ex;
          }
        }
      } catch (RuntimeException ex) {
        // 保存运行时异常，供主线程检查重抛
        synchronized (MultithreadedMapRunner.this) {
          if (MultithreadedMapRunner.this.runtimeException == null) {
            MultithreadedMapRunner.this.runtimeException = ex;
          }
        }
      }
    }
  }

}