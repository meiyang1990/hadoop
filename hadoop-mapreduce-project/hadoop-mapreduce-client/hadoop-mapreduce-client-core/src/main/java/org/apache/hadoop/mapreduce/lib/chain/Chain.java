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
package org.apache.hadoop.mapreduce.lib.chain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Stringifier;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;

/**
 * 文件说明：提供ChainMapper和ChainReducer的公共基础功能，实现MapReduce任务中多个处理阶段的链式执行
 * 核心职责：管理链式处理中各个Mapper/Reducer的配置、初始化、线程调度及数据传递
 * The Chain class provides all the common functionality for the
 * {@link ChainMapper} and the {@link ChainReducer} classes.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Chain {
  // 链式Mapper配置前缀
  protected static final String CHAIN_MAPPER = "mapreduce.chain.mapper";
  // 链式Reducer配置前缀
  protected static final String CHAIN_REDUCER = "mapreduce.chain.reducer";

  // Mapper数量配置后缀
  protected static final String CHAIN_MAPPER_SIZE = ".size";
  // Mapper类名配置后缀
  protected static final String CHAIN_MAPPER_CLASS = ".mapper.class.";
  // Mapper配置序列化后存储的配置后缀
  protected static final String CHAIN_MAPPER_CONFIG = ".mapper.config.";
  // Reducer类名配置后缀
  protected static final String CHAIN_REDUCER_CLASS = ".reducer.class";
  // Reducer配置序列化后存储的配置后缀
  protected static final String CHAIN_REDUCER_CONFIG = ".reducer.config";

  // Mapper输入key类型配置键
  protected static final String MAPPER_INPUT_KEY_CLASS = 
    "mapreduce.chain.mapper.input.key.class";
  // Mapper输入value类型配置键
  protected static final String MAPPER_INPUT_VALUE_CLASS = 
    "mapreduce.chain.mapper.input.value.class";
  // Mapper输出key类型配置键
  protected static final String MAPPER_OUTPUT_KEY_CLASS = 
    "mapreduce.chain.mapper.output.key.class";
  // Mapper输出value类型配置键
  protected static final String MAPPER_OUTPUT_VALUE_CLASS = 
    "mapreduce.chain.mapper.output.value.class";
  // Reducer输入key类型配置键
  protected static final String REDUCER_INPUT_KEY_CLASS = 
    "mapreduce.chain.reducer.input.key.class";
  // Reducer输入value类型配置键
  protected static final String REDUCER_INPUT_VALUE_CLASS = 
    "mapreduce.chain.reducer.input.value.class";
  // Reducer输出key类型配置键
  protected static final String REDUCER_OUTPUT_KEY_CLASS = 
    "mapreduce.chain.reducer.output.key.class";
  // Reducer输出value类型配置键
  protected static final String REDUCER_OUTPUT_VALUE_CLASS = 
    "mapreduce.chain.reducer.output.value.class";

  // 标识当前链式是Mapper链还是Reducer链
  protected boolean isMap;

  @SuppressWarnings("unchecked")
  private List<Mapper> mappers = new ArrayList<Mapper>();
  // 存储当前链中的Reducer实例
  private Reducer<?, ?, ?, ?> reducer;
  // 存储每个Mapper对应的独立配置
  private List<Configuration> confList = new ArrayList<Configuration>();
  // Reducer的独立配置
  private Configuration rConf;
  // 存储每个处理节点对应的执行线程
  private List<Thread> threads = new ArrayList<Thread>();
  // 存储各个处理节点之间传递数据的阻塞队列
  private List<ChainBlockingQueue<?>> blockingQueues = 
    new ArrayList<ChainBlockingQueue<?>>();
  // 存储执行过程中抛出的异常，用于线程间异常传递
  private Throwable throwable = null;

  /**
   * 构造Chain实例，根据参数指定是Mapper链还是Reducer链
   * 
   * @param isMap
   *          TRUE表示是Mapper链，FALSE表示是Reducer链
   */
  protected Chain(boolean isMap) {
    this.isMap = isMap;
  }

  /**
   * 存储链式处理中传递的键值对，也用于标记输入结束
   * @param <K> 键类型
   * @param <V> 值类型
   */
  static class KeyValuePair<K, V> {
    K key;
    V value;
    // 标记是否是输入结束标记
    boolean endOfInput;

    KeyValuePair(K key, V value) {
      this.key = key;
      this.value = value;
      this.endOfInput = false;
    }

    KeyValuePair(boolean eof) {
      this.key = null;
      this.value = null;
      this.endOfInput = eof;
    }
  }

  // ChainRecordReader either reads from blocking queue or task context.
  /**
   * 链式处理的RecordReader实现，支持从阻塞队列或原始任务上下文读取数据
   * @param <KEYIN> 输入键类型
   * @param <VALUEIN> 输入值类型
   */
  private static class ChainRecordReader<KEYIN, VALUEIN> extends
      RecordReader<KEYIN, VALUEIN> {
    private Class<?> keyClass;
    private Class<?> valueClass;
    private KEYIN key;
    private VALUEIN value;
    private Configuration conf;
    TaskInputOutputContext<KEYIN, VALUEIN, ?, ?> inputContext = null;
    ChainBlockingQueue<KeyValuePair<KEYIN, VALUEIN>> inputQueue = null;

    // 构造从阻塞队列读取数据的ChainRecordReader
    ChainRecordReader(Class<?> keyClass, Class<?> valueClass,
        ChainBlockingQueue<KeyValuePair<KEYIN, VALUEIN>> inputQueue,
        Configuration conf) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.inputQueue = inputQueue;
      this.conf = conf;
    }

    // 构造从原始任务上下文读取数据的ChainRecordReader
    ChainRecordReader(TaskInputOutputContext<KEYIN, VALUEIN, ?, ?> context) {
      inputContext = context;
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    /**
     * Advance to the next key, value pair, returning null if at end.
     * 
     * @return the key object that was read into, or null if no more
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (inputQueue != null) {
        // 从阻塞队列读取数据
        return readFromQueue();
      } else if (inputContext.nextKeyValue()) {
        // 从任务上下文读取原始输入数据
        this.key = inputContext.getCurrentKey();
        this.value = inputContext.getCurrentValue();
        return true;
      } else {
        return false;
      }
    }

    @SuppressWarnings("unchecked")
    // 从阻塞队列读取键值对
    private boolean readFromQueue() throws IOException, InterruptedException {
      KeyValuePair<KEYIN, VALUEIN> kv = null;

      // 等待队列输入
      kv = inputQueue.dequeue();
      if (kv.endOfInput) {
        // 读到输入结束标记，返回false
        return false;
      }
      // 新建对象拷贝键值对，避免不同阶段共享对象导致问题
      key = (KEYIN) ReflectionUtils.newInstance(keyClass, conf);
      value = (VALUEIN) ReflectionUtils.newInstance(valueClass, conf);
      ReflectionUtils.copy(conf, kv.key, this.key);
      ReflectionUtils.copy(conf, kv.value, this.value);
      return true;
    }

    /**
     * Get the current key.
     * 
     * @return the current key object or null if there isn't one
     * @throws IOException
     * @throws InterruptedException
     */
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
      return this.key;
    }

    /**
     * Get the current value.
     * 
     * @return the value object that was read into
     * @throws IOException
     * @throws InterruptedException
     */
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
      return this.value;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }
  }

  // ChainRecordWriter either writes to blocking queue or task context

  /**
   * 链式处理的RecordWriter实现，支持写入阻塞队列或原始任务上下文
   * @param <KEYOUT> 输出键类型
   * @param <VALUEOUT> 输出值类型
   */
  private static class ChainRecordWriter<KEYOUT, VALUEOUT> extends
      RecordWriter<KEYOUT, VALUEOUT> {
    TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> outputContext = null;
    ChainBlockingQueue<KeyValuePair<KEYOUT, VALUEOUT>> outputQueue = null;
    KEYOUT keyout;
    VALUEOUT valueout;
    Configuration conf;
    Class<?> keyClass;
    Class<?> valueClass;

    // 构造写入任务上下文的ChainRecordWriter
    ChainRecordWriter(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
      outputContext = context;
    }

    // 构造写入阻塞队列的ChainRecordWriter
    ChainRecordWriter(Class<?> keyClass, Class<?> valueClass,
        ChainBlockingQueue<KeyValuePair<KEYOUT, VALUEOUT>> output,
        Configuration conf) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.outputQueue = output;
      this.conf = conf;
    }

    /**
     * Writes a key/value pair.
     * 
     * @param key
     *          the key to write.
     * @param value
     *          the value to write.
     * @throws IOException
     */
    public void write(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {
      if (outputQueue != null) {
        // 写入阻塞队列，传递给下一个处理节点
        writeToQueue(key, value);
      } else {
        // 写入任务上下文，输出到最终结果
        outputContext.write(key, value);
      }
    }

    @SuppressWarnings("unchecked")
    // 将键值对写入阻塞队列
    private void writeToQueue(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {
      // 新建对象拷贝键值对，避免不同阶段共享对象导致问题
      this.keyout = (KEYOUT) ReflectionUtils.newInstance(keyClass, conf);
      this.valueout = (VALUEOUT) ReflectionUtils.newInstance(valueClass, conf);
      ReflectionUtils.copy(conf, key, this.keyout);
      ReflectionUtils.copy(conf, value, this.valueout);

      // 等待队列可用后写入
      outputQueue.enqueue(new KeyValuePair<KEYOUT, VALUEOUT>(keyout, valueout));
    }

    /**
     * Close this <code>RecordWriter</code> to future operations.
     * 
     * @param context
     *          the context of the task
     * @throws IOException
     */
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      if (outputQueue != null) {
        // 写入输入结束标记，通知下游处理节点没有更多数据
        outputQueue.enqueue(new KeyValuePair<KEYOUT, VALUEOUT>(true));
      }
    }

  }

  // 获取执行异常，线程安全方法
  private synchronized Throwable getThrowable() {
    return throwable;
  }

  // 仅在未设置异常时设置异常，用于避免多个线程重复设置异常
  private synchronized boolean setIfUnsetThrowable(Throwable th) {
    if (throwable == null) {
      throwable = th;
      return true;
    }
    return false;
  }

  /**
   * 单个Mapper的执行线程，在独立线程中运行Mapper逻辑
   * @param <KEYIN> Mapper输入键类型
   * @param <VALUEIN> Mapper输入值类型
   * @param <KEYOUT> Mapper输出键类型
   * @param <VALUEOUT> Mapper输出值类型
   */
  private class MapRunner<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends SubjectInheritingThread {
    private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper;
    private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context chainContext;
    private RecordReader<KEYIN, VALUEIN> rr;
    private RecordWriter<KEYOUT, VALUEOUT> rw;

    public MapRunner(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper,
        Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext,
        RecordReader<KEYIN, VALUEIN> rr, RecordWriter<KEYOUT, VALUEOUT> rw)
        throws IOException, InterruptedException {
      this.mapper = mapper;
      this.rr = rr;
      this.rw = rw;
      this.chainContext = mapperContext;
    }

    @Override
    // 执行Mapper业务逻辑
    public void work() {
      if (getThrowable() != null) {
        // 已有异常发生，直接退出
        return;
      }
      try {
        // 运行Mapper
        mapper.run(chainContext);
        rr.close();
        rw.close(chainContext);
      } catch (Throwable th) {
        // 捕获异常，中断所有其他线程
        if (setIfUnsetThrowable(th)) {
          interruptAllThreads();
        }
      }
    }
  }

  /**
   * 单个Reducer的执行线程，在独立线程中运行Reducer逻辑
   * @param <KEYIN> Reducer输入键类型
   * @param <VALUEIN> Reducer输入值类型
   * @param <KEYOUT> Reducer输出键类型
   * @param <VALUEOUT> Reducer输出值类型
   */
  private class ReduceRunner<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends SubjectInheritingThread {
    private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer;
    private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context chainContext;
    private RecordWriter<KEYOUT, VALUEOUT> rw;

    ReduceRunner(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context,
        Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer,
        RecordWriter<KEYOUT, VALUEOUT> rw) throws IOException,
        InterruptedException {
      this.reducer = reducer;
      this.chainContext = context;
      this.rw = rw;
    }

    @Override
    // 执行Reducer业务逻辑
    public void work() {
      try {
        reducer.run(chainContext);
        rw.close(chainContext);
      } catch (Throwable th) {
        // 捕获异常，中断所有其他线程
        if (setIfUnsetThrowable(th)) {
          interruptAllThreads();
        }
      }
    }
  }

  // 获取指定索引Mapper的配置
  Configuration getConf(int index) {
    return confList.get(index);
  }

  /**
   * 创建链式Mapper的Context，包装自定义的RecordReader和RecordWriter
   */
  private <KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context createMapContext(
      RecordReader<KEYIN, VALUEIN> rr, RecordWriter<KEYOUT, VALUEOUT> rw,
      TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context,
      Configuration conf) {
    MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext = 
      new ChainMapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(
        context, rr, rw, conf);
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext = 
      new WrappedMapper<KEYIN, VALUEIN