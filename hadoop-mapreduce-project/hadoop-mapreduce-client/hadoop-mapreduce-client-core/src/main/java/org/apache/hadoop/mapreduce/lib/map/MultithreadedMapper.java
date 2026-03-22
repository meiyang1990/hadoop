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

package org.apache.hadoop.mapreduce.lib.map;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 多线程版本的Mapper实现，用于IO密集型Map任务提升吞吐
 * <p>
 * 当Map操作不是CPU密集型时（例如依赖外部IO操作），可以使用该实现代替默认实现提升吞吐量
 * <p>
 * 使用该MapRunnable的用户自定义Mapper必须是线程安全的
 * <p>
 * 需要通过 {@link #setMapperClass(Job, Class)} 配置实际执行的Mapper类，
 * 通过 {@link #getNumberOfThreads(JobContext)} 配置线程池线程数，默认值为10个线程
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultithreadedMapper<K1, V1, K2, V2> 
  extends Mapper<K1, V1, K2, V2> {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultithreadedMapper.class);
  public static String NUM_THREADS = "mapreduce.mapper.multithreadedmapper.threads";
  public static String MAP_CLASS = "mapreduce.mapper.multithreadedmapper.mapclass";
  
  private Class<? extends Mapper<K1,V1,K2,V2>> mapClass;
  private Context outer;
  private List<MapRunner> runners;

  /**
   * 从作业配置中获取线程池的线程数量
   * @param job 作业对象
   * @return 配置的线程数量，默认返回10
   */
  public static int getNumberOfThreads(JobContext job) {
    return job.getConfiguration().getInt(NUM_THREADS, 10);
  }

  /**
   * 设置线程池运行Map任务的线程数量
   * @param job 要修改的作业对象
   * @param threads 新的线程数量
   */
  public static void setNumberOfThreads(Job job, int threads) {
    job.getConfiguration().setInt(NUM_THREADS, threads);
  }

  /**
   * 从作业配置中获取实际执行逻辑的Mapper类
   * @param <K1> Map输入key类型
   * @param <V1> Map输入value类型
   * @param <K2> Map输出key类型
   * @param <V2> Map输出value类型
   * @param job 作业对象
   * @return 配置的Mapper类，默认返回Mapper基类
   */
  @SuppressWarnings("unchecked")
  public static <K1,V1,K2,V2>
  Class<Mapper<K1,V1,K2,V2>> getMapperClass(JobContext job) {
    return (Class<Mapper<K1,V1,K2,V2>>) 
      job.getConfiguration().getClass(MAP_CLASS, Mapper.class);
  }
  
  /**
   * 设置实际执行逻辑的Mapper类
   * @param <K1> Map输入key类型
   * @param <V1> Map输入value类型
   * @param <K2> Map输出key类型
   * @param <V2> Map输出value类型
   * @param job 要修改的作业对象
   * @param cls 作为实际Mapper的类
   */
  public static <K1,V1,K2,V2> 
  void setMapperClass(Job job, 
                      Class<? extends Mapper<K1,V1,K2,V2>> cls) {
    if (MultithreadedMapper.class.isAssignableFrom(cls)) {
      throw new IllegalArgumentException("Can't have recursive " + 
                                         "MultithreadedMapper instances.");
    }
    job.getConfiguration().setClass(MAP_CLASS, cls, Mapper.class);
  }

  /**
   * 使用线程池多线程执行用户Map任务
   */
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    outer = context;
    // 获取配置的线程数量
    int numberOfThreads = getNumberOfThreads(context);
    // 获取实际执行的Mapper类
    mapClass = getMapperClass(context);
    // 调试日志输出配置信息
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configuring multithread runner to use " + numberOfThreads + 
                " threads");
    }
    
    // 初始化MapRunner线程列表
    runners =  new ArrayList<MapRunner>(numberOfThreads);
    // 启动所有工作线程
    for(int i=0; i < numberOfThreads; ++i) {
      MapRunner thread = new MapRunner(context);
      thread.start();
      runners.add(i, thread);
    }
    // 等待所有线程执行完成，收集异常
    for(int i=0; i < numberOfThreads; ++i) {
      MapRunner thread = runners.get(i);
      thread.join();
      Throwable th = thread.throwable;
      // 如果线程抛出异常，向上抛出
      if (th != null) {
        if (th instanceof IOException) {
          throw (IOException) th;
        } else if (th instanceof InterruptedException) {
          throw (InterruptedException) th;
        } else {
          throw new RuntimeException(th);
        }
      }
    }
  }

  /**
   * 为子线程Mapper提供的RecordReader包装类，保证读取时线程安全
   * @param <K1> 输入key类型
   * @param <V1> 输入value类型
   */
  private class SubMapRecordReader extends RecordReader<K1,V1> {
    private K1 key;
    private V1 value;
    private Configuration conf;

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void initialize(InputSplit split, 
                           TaskAttemptContext context
                           ) throws IOException, InterruptedException {
      conf = context.getConfiguration();
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      // 对外部上下文加锁，保证多线程读取安全
      synchronized (outer) {
        if (!outer.nextKeyValue()) {
          return false;
        }
        // 拷贝当前键值对给子线程
        key = ReflectionUtils.copy(outer.getConfiguration(),
                                   outer.getCurrentKey(), key);
        value = ReflectionUtils.copy(conf, outer.getCurrentValue(), value);
        return true;
      }
    }

    public K1 getCurrentKey() {
      return key;
    }

    @Override
    public V1 getCurrentValue() {
      return value;
    }
  }
  
  /**
   * 为子线程Mapper提供的RecordWriter包装类，保证输出时线程安全
   * @param <K2> 输出key类型
   * @param <V2> 输出value类型
   */
  private class SubMapRecordWriter extends RecordWriter<K2,V2> {

    @Override
    public void close(TaskAttemptContext context) throws IOException,
                                                 InterruptedException {
    }

    @Override
    public void write(K2 key, V2 value) throws IOException,
                                               InterruptedException {
      // 对外部上下文加锁，保证多线程输出安全
      synchronized (outer) {
        outer.write(key, value);
      }
    }  
  }

  /**
   * 为子线程Mapper提供的状态上报包装类，代理到外部上下文
   */
  private class SubMapStatusReporter extends StatusReporter {

    @Override
    public Counter getCounter(Enum<?> name) {
      return outer.getCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return outer.getCounter(group, name);
    }

    @Override
    public void progress() {
      outer.progress();
    }

    @Override
    public void setStatus(String status) {
      outer.setStatus(status);
    }
    
    @Override
    public float getProgress() {
      return outer.getProgress();
    }
  }

  /**
   * 实际执行用户Map任务的工作线程，继承SubjectInheritingThread继承访问主体
   */
  private class MapRunner extends SubjectInheritingThread {
    private Mapper<K1,V1,K2,V2> mapper;
    private Context subcontext;
    private Throwable throwable;
    private RecordReader<K1,V1> reader = new SubMapRecordReader();

    /**
     * 构造MapRunner工作线程，初始化实际Mapper和子上下文
     * @param context 外部Mapper上下文
     * @throws IOException 初始化IO异常
     * @throws InterruptedException 中断异常
     */
    MapRunner(Context context) throws IOException, InterruptedException {
      // 反射实例化用户Mapper
      mapper = ReflectionUtils.newInstance(mapClass, 
                                           context.getConfiguration());
      // 构造子Map上下文，注入包装好的Reader、Writer和Reporter
      MapContext<K1, V1, K2, V2> mapContext = 
        new MapContextImpl<K1, V1, K2, V2>(outer.getConfiguration(), 
                                           outer.getTaskAttemptID(),
                                           reader,
                                           new SubMapRecordWriter(), 
                                           context.getOutputCommitter(),
                                           new SubMapStatusReporter(),
                                           outer.getInputSplit());
      subcontext = new WrappedMapper<K1, V1, K2, V2>().getMapContext(mapContext);
      reader.initialize(context.getInputSplit(), context);
    }

    /**
     * 线程工作方法，执行Mapper逻辑并捕获异常
     */
    @Override
    public void work() {
      try {
        mapper.run(subcontext);
        reader.close();
      } catch (Throwable ie) {
        // 保存异常供主线程处理
        throwable = ie;
      }
    }
  }

}