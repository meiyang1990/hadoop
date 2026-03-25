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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.util.Progressable;

/**
 * 支持MapReduce作业向默认输出之外输出到多个命名输出文件，每个命名输出可配置独立的OutputFormat、键值类型
 * 支持两种输出类型：单个输出（一个命名输出对应一个文件前缀）和多输出（一个命名输出可生成多个自定义命名的文件）
 * 可开启计数器统计每个命名输出的记录条数，默认计数器关闭
 * 在Mapper中使用时，写入MultipleOutputs的数据不会进入Reduce阶段，只有写入默认OutputCollector的数据会参与Shuffle和Reduce
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultipleOutputs {

  // 配置项：存储所有命名输出名称，空格分隔
  private static final String NAMED_OUTPUTS = "mo.namedOutputs";

  // 命名输出配置前缀
  private static final String MO_PREFIX = "mo.namedOutput.";

  // 配置后缀：OutputFormat类
  private static final String FORMAT = ".format";
  // 配置后缀：键类型
  private static final String KEY = ".key";
  // 配置后缀：值类型
  private static final String VALUE = ".value";
  // 配置后缀：是否为多输出
  private static final String MULTI = ".multi";

  // 配置项：是否开启计数器
  private static final String COUNTERS_ENABLED = "mo.counters";

  /**
   * 多个输出计数器所属计数器组名，使用当前类全限定名
   */
  private static final String COUNTERS_GROUP = MultipleOutputs.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(MultipleOutputs.class);

  /**
   * 检查命名输出是否已存在/不存在，不符合预期则抛出异常
   * @param conf 作业配置
   * @param namedOutput 要检查的命名输出名称
   * @param alreadyDefined 预期是否已存在，true要求该输出已存在，false要求该输出不存在
   * @throws IllegalArgumentException 检查不通过时抛出
   */
  private static void checkNamedOutput(JobConf conf, String namedOutput,
                                       boolean alreadyDefined) {
    List<String> definedChannels = getNamedOutputsList(conf);
    if (alreadyDefined && definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' already alreadyDefined");
    } else if (!alreadyDefined && !definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' not defined");
    }
  }

  /**
   * 检查命名输出名称是否合法，仅允许字母数字
   * @param namedOutput 要检查的命名输出名称
   * @throws IllegalArgumentException 名称包含非法字符时抛出
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
   * 检查命名输出名称是否合法，不能为保留名称"part"
   * @param namedOutput 要检查的命名输出名称
   * @throws IllegalArgumentException 名称非法时抛出
   */
  private static void checkNamedOutputName(String namedOutput) {
    checkTokenName(namedOutput);
    // name cannot be the name used for the default output
    if (namedOutput.equals("part")) {
      throw new IllegalArgumentException(
        "Named output name cannot be 'part'");
    }
  }

  /**
   * 从作业配置中解析所有已定义的命名输出名称列表
   * @param conf 作业配置
   * @return 命名输出名称列表
   */
  public static List<String> getNamedOutputsList(JobConf conf) {
    List<String> names = new ArrayList<String>();
    StringTokenizer st = new StringTokenizer(conf.get(NAMED_OUTPUTS, ""), " ");
    while (st.hasMoreTokens()) {
      names.add(st.nextToken());
    }
    return names;
  }


  /**
   * 判断指定命名输出是否为多输出类型
   * @param conf 作业配置
   * @param namedOutput 命名输出名称
   * @return true表示是多输出，false表示是单输出，输出不存在则返回false
   */
  public static boolean isMultiNamedOutput(JobConf conf, String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getBoolean(MO_PREFIX + namedOutput + MULTI, false);
  }

  /**
   * 获取指定命名输出配置的OutputFormat类
   * @param conf 作业配置
   * @param namedOutput 命名输出名称
   * @return 命名输出的OutputFormat类
   */
  public static Class<? extends OutputFormat> getNamedOutputFormatClass(
    JobConf conf, String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getClass(MO_PREFIX + namedOutput + FORMAT, null,
      OutputFormat.class);
  }

  /**
   * 获取指定命名输出配置的键类型
   * @param conf 作业配置
   * @param namedOutput 命名输出名称
   * @return 命名输出的键类
   */
  public static Class<?> getNamedOutputKeyClass(JobConf conf,
                                                String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getClass(MO_PREFIX + namedOutput + KEY, null,
	Object.class);
  }

  /**
   * 获取指定命名输出配置的值类型
   * @param conf 作业配置
   * @param namedOutput 命名输出名称
   * @return 命名输出的值类
   */
  public static Class<?> getNamedOutputValueClass(JobConf conf,
                                                  String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getClass(MO_PREFIX + namedOutput + VALUE, null,
      Object.class);
  }

  /**
   * 向作业配置添加一个单命名输出
   * @param conf 作业配置对象
   * @param namedOutput 命名输出名称，仅允许字母数字，不能是保留名part
   * @param outputFormatClass 输出格式类
   * @param keyClass 键类型
   * @param valueClass 值类型
   */
  public static void addNamedOutput(JobConf conf, String namedOutput,
                                Class<? extends OutputFormat> outputFormatClass,
                                Class<?> keyClass, Class<?> valueClass) {
    addNamedOutput(conf, namedOutput, false, outputFormatClass, keyClass,
      valueClass);
  }

  /**
   * 向作业配置添加一个多命名输出，允许一个命名输出生成多个自定义名称的文件
   * @param conf 作业配置对象
   * @param namedOutput 命名输出名称，仅允许字母数字，不能是保留名part
   * @param outputFormatClass 输出格式类
   * @param keyClass 键类型
   * @param valueClass 值类型
   */
  public static void addMultiNamedOutput(JobConf conf, String namedOutput,
                               Class<? extends OutputFormat> outputFormatClass,
                               Class<?> keyClass, Class<?> valueClass) {
    addNamedOutput(conf, namedOutput, true, outputFormatClass, keyClass,
      valueClass);
  }

  /**
   * 内部通用方法：向作业配置添加一个命名输出，区分单输出和多输出
   * @param conf 作业配置对象
   * @param namedOutput 命名输出名称
   * @param multi 是否为多输出
   * @param outputFormatClass 输出格式类
   * @param keyClass 键类型
   * @param valueClass 值类型
   */
  private static void addNamedOutput(JobConf conf, String namedOutput,
                               boolean multi,
                               Class<? extends OutputFormat> outputFormatClass,
                               Class<?> keyClass, Class<?> valueClass) {
    checkNamedOutputName(namedOutput);
    checkNamedOutput(conf, namedOutput, true);
    conf.set(NAMED_OUTPUTS, conf.get(NAMED_OUTPUTS, "") + " " + namedOutput);
    conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass,
      OutputFormat.class);
    conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
    conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
    conf.setBoolean(MO_PREFIX + namedOutput + MULTI, multi);
  }

  /**
   * 设置是否开启命名输出计数器，默认关闭
   * 计数器命名规则：单输出直接使用输出名，多输出为 输出名_多文件名
   * @param conf 作业配置
   * @param enabled true开启，false关闭
   */
  public static void setCountersEnabled(JobConf conf, boolean enabled) {
    conf.setBoolean(COUNTERS_ENABLED, enabled);
  }

  /**
   * 获取命名输出计数器是否开启
   * @param conf 作业配置
   * @return true开启，false关闭，默认返回false
   */
  public static boolean getCountersEnabled(JobConf conf) {
    return conf.getBoolean(COUNTERS_ENABLED, false);
  }

  // instance code, to be used from Mapper/Reducer code

  private JobConf conf;
  private OutputFormat outputFormat;
  private Set<String> namedOutputs;
  private Map<String, RecordWriter> recordWriters;
  private boolean countersEnabled;

  @VisibleForTesting
  synchronized void setRecordWriters(Map<String, RecordWriter> recordWriters) {
    this.recordWriters = recordWriters;
  }

  /**
   * 构造MultipleOutputs实例，初始化所有命名输出，需要在Mapper/Reducer的configure方法中调用
   * @param job 作业配置对象
   */
  public MultipleOutputs(JobConf job) {
    this.conf = job;
    outputFormat = new InternalFileOutputFormat();
    namedOutputs = Collections.unmodifiableSet(
      new HashSet<String>(MultipleOutputs.getNamedOutputsList(job)));
    recordWriters = new HashMap<String, RecordWriter>();
    countersEnabled = getCountersEnabled(job);
  }

  /**
   * 获取所有已定义命名输出名称的迭代器
   * @return 命名输出名称迭代器
   */
  public Iterator<String> getNamedOutputs() {
    return namedOutputs.iterator();
  }


  // by being synchronized MultipleOutputTask can be use with a
  // MultithreaderMapRunner.
  /**
   * 获取指定输出文件名对应的RecordWriter，不存在则创建并缓存
   * 线程安全，支持多线程Mapper使用
   * @param namedOutput 命名输出名称
   * @param baseFileName 基础文件名
   * @param reporter 报告器，用于计数器更新
   * @return 对应RecordWriter实例
   * @throws IOException 创建RecordWriter失败时抛出
   */
  private synchronized RecordWriter getRecordWriter(String namedOutput,
                                                    String baseFileName,
                                                    final Reporter reporter)
    throws IOException {
    RecordWriter writer = recordWriters.get(baseFileName);
    if (writer == null) {
      if (countersEnabled && reporter == null) {
        throw new IllegalArgumentException(
          "Counters are enabled, Reporter cannot be NULL");
      }
      // 创建新配置，注入当前命名输出信息
      JobConf jobConf = new JobConf(conf);
      jobConf.set(InternalFileOutputFormat.CONFIG_NAMED_OUTPUT, namedOutput);
      FileSystem fs = FileSystem.get(conf);
      writer =
        outputFormat.getRecordWriter(fs, jobConf, baseFileName, reporter);

      if (countersEnabled) {
        if (reporter == null) {
          throw new IllegalArgumentException(
            "Counters are enabled, Reporter cannot be NULL");
        }
        // 包装计数器，每次写入递增计数
        writer = new RecordWriterWithCounter(writer, baseFileName, reporter);
      }

      recordWriters.put(baseFileName, writer);
    }
    return writer;
  }

  /**
   * RecordWriter包装类，每次写入时递增对应计数器
   */
  private static class RecordWriterWithCounter implements RecordWriter {
    private RecordWriter writer;
    private String counterName;
    private Reporter reporter;

    public RecordWriterWithCounter(RecordWriter writer, String counterName,
                                   Reporter reporter) {
      this.writer = writer;
      this.counterName = counterName;
      this.reporter = reporter;
    }

    @SuppressWarnings({"unchecked"})
    public void write(Object key, Object value) throws IOException {
      // 递增计数器
      reporter.incrCounter(COUNTERS_GROUP, counterName, 1);
      writer.write(key, value);
    }

    public void close(Reporter reporter) throws IOException {
      writer.close(reporter);
    }
  }

  /**
   * 获取单命名输出对应的OutputCollector
   * @param namedOutput 命名输出名称
   * @param reporter 报告器
   * @return 输出收集器
   * @throws IOException 获取RecordWriter失败时抛出
   */
  @SuppressWarnings({"unchecked"})
  public OutputCollector getCollector(String namedOutput, Reporter reporter)
    throws IOException {
    return getCollector(namedOutput, null, reporter);
  }

  /**
   * 获取命名输出对应的OutputCollector，支持多输出指定子名称
   * @param namedOutput 命名输出名称
   * @param multiName 多输出的子名称，单输出需传null
   * @param reporter 报告器
   * @return 输出收集器
   * @throws IOException 参数非法或获取RecordWriter失败时抛出
   */
  @SuppressWarnings({"unchecked"})
  public OutputCollector getCollector(String namedOutput, String multiName,
                                      Reporter reporter)
    throws IOException {

    checkNamedOutputName(namedOutput);
    if (!namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" +
        namedOutput + "'");
    }
    boolean multi = isMultiNamedOutput(conf, namedOutput);

    // 单输出不能指定子名称
    if (!multi && multiName != null) {
      throw new IllegalArgumentException("Name output '" + namedOutput +
        "' has not been defined as multi");
    }
    // 多输出需要检查子名称合法性
    if (multi) {
      checkTokenName(multiName);
    }

    // 生成基础文件名：多输出为 命名输出_子名称，单输出为命名输出
    String baseFileName = (multi) ? namedOutput + "_" + multiName : namedOutput;

    final RecordWriter writer =
      getRecordWriter(namedOutput, baseFileName, reporter);

    // 返回匿名OutputCollector，直接委托给RecordWriter写入
    return new OutputCollector() {

      @SuppressWarnings({"unchecked"})
      public void collect(Object key, Object value) throws IOException {
        writer.write(key, value);
      }

    };
  }

  /**
   * 关闭所有已打开的RecordWriter，释放资源，需要在Mapper/Reducer的close方法中调用
   * 使用多线程并行关闭提升大量输出时的关闭速度
   * @throws IOException 任意一个关闭操作出现异常时抛出
   */
  public void close() throws IOException {
    // 从配置获取关闭线程数，默认使用配置默认值
    int nThreads = conf.getInt(MRConfig