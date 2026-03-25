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
package org.apache.hadoop.mapreduce.lib.output;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @file MultipleOutputs.java
 * @brief MapReduce多输出输出工具类，支持将输出写入多个命名输出或自定义路径
 * 
 * 核心功能：
 * 1. 支持在作业默认输出之外，添加多个额外的命名输出，每个输出可配置独立的OutputFormat、键值类型
 * 2. 支持根据用户自定义路径将输出写入不同文件，实现按规则分文件输出
 * 3. 支持对每个输出记录写入计数，默认关闭计数器
 * 
 * 两种典型使用场景：
 * - 场景一：定义多个额外命名输出，每个输出使用独立格式和类型，输出文件自动命名
 * - 场景二：用户自定义输出路径，将不同数据写入不同文件/目录，实现动态输出分片
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultipleOutputs<KEYOUT, VALUEOUT> {

  private static final String MULTIPLE_OUTPUTS = "mapreduce.multipleoutputs";

  private static final String MO_PREFIX = 
    "mapreduce.multipleoutputs.namedOutput.";

  private static final String FORMAT = ".format";
  private static final String KEY = ".key";
  private static final String VALUE = ".value";
  private static final String COUNTERS_ENABLED = 
    "mapreduce.multipleoutputs.counters";

  /**
   * MultipleOutputs计数器所在的计数器组名称
   */
  private static final String COUNTERS_GROUP = MultipleOutputs.class.getName();
  private static final Logger LOG =
      LoggerFactory.getLogger(org.apache.hadoop.mapred.lib.MultipleOutputs.class);

  /**
   * 命名输出对应的TaskAttemptContext缓存，避免重复创建
   */
  private Map<String, TaskAttemptContext> taskContexts = new HashMap<String, TaskAttemptContext>();
  /**
   * 使用作业默认输出格式的缓存上下文
   */
  private TaskAttemptContext jobOutputFormatContext;

  /**
   * @brief 检查命名输出名称是否合法，仅允许字母数字
   * @param namedOutput 命名输出名称
   * @throws IllegalArgumentException 名称不合法时抛出
   */
  private static void checkTokenName(String namedOutput) {
    if (namedOutput == null || namedOutput.length() == 0) {
      throw new IllegalArgumentException(
        "Name cannot be NULL or emtpy");
    }
    for (char ch : namedOutput.toCharArray()) {
      if ((ch >= 'A') && (ch <= 'Z')) {
        continue;
      }
      if ((ch >= 'a') && (ch <= 'z')) {
        continue;
      }
      if ((ch >= '0') && (ch <= '9')) {
        continue;
      }
      throw new IllegalArgumentException(
        "Name cannot be have a '" + ch + "' char");
    }
  }

  /**
   * @brief 检查基础输出路径是否合法，不能使用默认输出保留名"part"
   * @param outputPath 基础输出路径名称
   * @throws IllegalArgumentException 名称不合法时抛出
   */
  private static void checkBaseOutputPath(String outputPath) {
    if (outputPath.equals(FileOutputFormat.PART)) {
      throw new IllegalArgumentException("output name cannot be 'part'");
    }
  }
  
  /**
   * @brief 检查命名输出名称整体合法性，包括格式检查和重复定义检查
   * @param job 作业上下文
   * @param namedOutput 命名输出名称
   * @param alreadyDefined 是否已定义标志，true表示检查是否重复，false表示检查是否已定义
   * @throws IllegalArgumentException 检查不通过时抛出
   */
  private static void checkNamedOutputName(JobContext job,
      String namedOutput, boolean alreadyDefined) {
    checkTokenName(namedOutput);
    checkBaseOutputPath(namedOutput);
    List<String> definedChannels = getNamedOutputsList(job);
    if (alreadyDefined && definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' already alreadyDefined");
    } else if (!alreadyDefined && !definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' not defined");
    }
  }

  /**
   * @brief 从作业配置中读取所有已定义的命名输出名称列表
   * @param job 作业上下文
   * @return 命名输出名称列表
   */
  // Returns list of channel names.
  private static List<String> getNamedOutputsList(JobContext job) {
    List<String> names = new ArrayList<String>();
    StringTokenizer st = new StringTokenizer(
      job.getConfiguration().get(MULTIPLE_OUTPUTS, ""), " ");
    while (st.hasMoreTokens()) {
      names.add(st.nextToken());
    }
    return names;
  }

  /**
   * @brief 从作业配置中获取指定命名输出的OutputFormat类
   * @param job 作业上下文
   * @param namedOutput 命名输出名称
   * @return OutputFormat类对象
   */
  // Returns the named output OutputFormat.
  @SuppressWarnings("unchecked")
  private static Class<? extends OutputFormat<?, ?>> getNamedOutputFormatClass(
    JobContext job, String namedOutput) {
    return (Class<? extends OutputFormat<?, ?>>)
      job.getConfiguration().getClass(MO_PREFIX + namedOutput + FORMAT, null,
      OutputFormat.class);
  }

  /**
   * @brief 从作业配置中获取指定命名输出的键类型
   * @param job 作业上下文
   * @param namedOutput 命名输出名称
   * @return 键类对象
   */
  // Returns the key class for a named output.
  private static Class<?> getNamedOutputKeyClass(JobContext job,
                                                String namedOutput) {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + KEY, null,
      Object.class);
  }

  /**
   * @brief 从作业配置中获取指定命名输出的值类型
   * @param job 作业上下文
   * @param namedOutput 命名输出名称
   * @return 值类对象
   */
  // Returns the value class for a named output.
  private static Class<?> getNamedOutputValueClass(
      JobContext job, String namedOutput) {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + VALUE,
      null, Object.class);
  }

  /**
   * @brief 向作业添加一个命名输出配置
   * 
   * @param job               目标作业对象
   * @param namedOutput       命名输出名称，仅允许字母数字，不能为"part"
   * @param outputFormatClass 该输出使用的OutputFormat类
   * @param keyClass          该输出使用的键类型
   * @param valueClass        该输出使用的值类型
   */
  @SuppressWarnings("unchecked")
  public static void addNamedOutput(Job job, String namedOutput,
      Class<? extends OutputFormat> outputFormatClass,
      Class<?> keyClass, Class<?> valueClass) {
    checkNamedOutputName(job, namedOutput, true);
    Configuration conf = job.getConfiguration();
    conf.set(MULTIPLE_OUTPUTS,
      conf.get(MULTIPLE_OUTPUTS, "") + " " + namedOutput);
    conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass,
      OutputFormat.class);
    conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
    conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
  }

  /**
   * @brief 设置是否启用多个输出的计数器功能
   * 
   * 计数器会统计每个输出写入的记录数量，默认关闭
   * 计数器组为MultipleOutputs类名，计数器名称与输出名称一致
   *
   * @param job    目标作业对象
   * @param enabled true启用，false禁用
   */
  public static void setCountersEnabled(Job job, boolean enabled) {
    job.getConfiguration().setBoolean(COUNTERS_ENABLED, enabled);
  }

  /**
   * @brief 获取计数器是否启用的配置
   * @param job 作业上下文
   * @return true启用，false禁用，默认禁用
   */
  public static boolean getCountersEnabled(JobContext job) {
    return job.getConfiguration().getBoolean(COUNTERS_ENABLED, false);
  }

  @VisibleForTesting
  synchronized void setRecordWriters(Map<String, RecordWriter<?, ?>> recordWriters) {
    this.recordWriters = recordWriters;
  }

  /**
   * @brief 包装RecordWriter，实现写入时计数器递增
   */
  @SuppressWarnings("unchecked")
  private static class RecordWriterWithCounter extends RecordWriter {
    private RecordWriter writer;
    private String counterName;
    private TaskInputOutputContext context;

    /**
     * @brief 构造带计数器的RecordWriter包装器
     * @param writer 原始RecordWriter
     * @param counterName 计数器名称
     * @param context 任务上下文，用于获取计数器
     */
    public RecordWriterWithCounter(RecordWriter writer, String counterName,
                                   TaskInputOutputContext context) {
      this.writer = writer;
      this.counterName = counterName;
      this.context = context;
    }

    @SuppressWarnings({"unchecked"})
    public void write(Object key, Object value) 
        throws IOException, InterruptedException {
      // 写入前计数器加1
      context.getCounter(COUNTERS_GROUP, counterName).increment(1);
      writer.write(key, value);
    }

    public void close(TaskAttemptContext context) 
        throws IOException, InterruptedException {
      writer.close(context);
    }
  }

  // instance code, to be used from Mapper/Reducer code

  private TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context;
  private Set<String> namedOutputs;
  private Map<String, RecordWriter<?, ?>> recordWriters;
  private boolean countersEnabled;
  
  /**
   * @brief 构造MultipleOutputs实例，应在Mapper/Reducer的setup方法中调用初始化
   * @param context 任务输入输出上下文
   */
  public MultipleOutputs(
      TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
    this.context = context;
    namedOutputs = Collections.unmodifiableSet(
      new HashSet<String>(MultipleOutputs.getNamedOutputsList(context)));
    recordWriters = new HashMap<String, RecordWriter<?, ?>>();
    countersEnabled = getCountersEnabled(context);
  }

  /**
   * @brief 将键值对写入指定命名输出，使用命名输出默认输出路径
   * 
   * 输出文件名格式为 {namedOutput}-(m|r)-{part-number}
   *
   * @param namedOutput 目标命名输出名称
   * @param key         输出键
   * @param value       输出值
   */
  @SuppressWarnings("unchecked")
  public <K, V> void write(String namedOutput, K key, V value)
      throws IOException, InterruptedException {
    write(namedOutput, key, value, namedOutput);
  }

  /**
   * @brief 将键值对写入指定命名输出的自定义基础路径
   * 
   * 框架会为基础路径生成唯一的分片文件名
   *
   * @param namedOutput    目标命名输出名称
   * @param key            输出键
   * @param value          输出值
   * @param baseOutputPath 自定义基础输出路径，可包含斜杠创建子目录
   * <b>警告</b>：如果基础路径解析后位于作业最终输出目录之外，目录会被立即创建并在任务重试后保留，会破坏输出提交语义
   */
  @SuppressWarnings("unchecked")
  public <K, V> void write(String namedOutput, K key, V value,
      String baseOutputPath) throws IOException, InterruptedException {
    checkNamedOutputName(context, namedOutput, false);
    checkBaseOutputPath(baseOutputPath);
    if (!namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" +
        namedOutput + "'");
    }
    TaskAttemptContext taskContext = getContext(namedOutput);
    getRecordWriter(taskContext, baseOutputPath).write(key, value);
  }

  /**
   * @brief 使用作业默认输出格式，将键值对写入自定义基础输出路径
   * 
   * 无需提前定义命名输出，直接使用作业配置的默认输出格式
   * 作业默认OutputFormat必须是FileOutputFormat子类
   *
   * @param key            输出键
   * @param value          输出值
   * @param baseOutputPath 自定义基础输出路径，可包含斜杠创建子目录
   * <b>警告</b>：如果基础路径解析后位于作业最终输出目录之外，目录会被立即创建并在任务重试后保留，会破坏输出提交语义
   */
  @SuppressWarnings("unchecked")
  public void write(KEYOUT key, VALUEOUT value, String baseOutputPath) 
      throws IOException, InterruptedException {
    checkBaseOutputPath(baseOutputPath);
    if (jobOutputFormatContext == null) {
      // 创建包装后的上下文，复用原任务ID和配置，使用原上下文状态上报
      jobOutputFormatContext = 
        new TaskAttemptContextImpl(context.getConfiguration(), 
                                   context.getTaskAttemptID(),
                                   new WrappedStatusReporter(context));
    }
    getRecordWriter(jobOutputFormatContext, baseOutputPath).write(key, value);
  }

  /**
   * @brief 获取指定输出路径的RecordWriter，优先从缓存获取，不存在则创建
   * 
   * 方法同步保证多线程Mapper下的线程安全
   *
   * @param taskContext  任务上下文
   * @param baseFileName 基础输出文件名/路径
   * @return 对应路径的RecordWriter实例
   * @throws IOException IO异常或类加载异常时抛出
   * @throws InterruptedException 中断异常
   */
  // by being synchronized MultipleOutputTask can be use with a
  // MultithreadedMapper.
  @SuppressWarnings("unchecked")
  private synchronized RecordWriter getRecordWriter(
      TaskAttemptContext taskContext, String baseFileName) 
      throws IOException, InterruptedException {
    
    // 优先从缓存获取已创建的RecordWriter
    RecordWriter writer = recordWriters.get(baseFileName);
    
    // 缓存未命中，创建新的RecordWriter
    if (writer == null) {
      // 设置当前输出名称，用于生成最终文件名
      FileOutputFormat.setOutputName(taskContext, baseFileName);
      try {
        // 反射创建OutputFormat实例，获取RecordWriter
        writer = ((OutputFormat) ReflectionUtils.newInstance(
          taskContext.getOutputFormatClass(), taskContext.getConfiguration()))
          .getRecordWriter(taskContext);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
 
      // 如果启用计数器，包装RecordWriter添加计数功能
      if (countersEnabled) {
        writer = new RecordWriterWithCounter(writer, baseFileName, context);
      }
      
      // 将创建好的RecordWriter加入缓存
      recordWriters.put(baseFileName, writer);
    }
    return writer;
  }

   /**
    * @brief 为指定命名输出创建TaskAttemptContext，配置对应输出格式和键值类型
    * @param nameOutput 命名输出名称
    * @return 配置好的TaskAttemptContext实例
    * @throws IOException 创建过程IO异常
    */
  private TaskAttemptContext getContext(String nameOutput) throws IOException {
      
    TaskAttemptContext taskContext = taskContexts.get(nameOutput);
    
    // 缓存命中直接返回
    if (taskContext != null) {
        return taskContext;
    }
    
    // 通过创建新Job实例的方式，复用OutputFormat的现有初始化逻辑，支持任意输出格式
    Job job = Job.getInstance(context.getConfiguration());
    job.setOutputFormatClass(getNamedOutputFormatClass(context, nameOutput));
    job.setOutputKeyClass(getNamedOutputKeyClass(context, nameOutput));
    job.setOutputValueClass(getNamedOutputValueClass(context, nameOutput));
    taskContext = new TaskAttemptContextImpl(job.getConfiguration(), context
        .getTaskAttemptID