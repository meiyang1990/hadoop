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

package org.apache.hadoop.mapred.pipes;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.StringTokenizer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop Pipes框架的作业提交入口类，支持命令行和API两种方式提交C++编写的MapReduce作业。
 * Pipes是Hadoop对C++ MapReduce程序的支持框架，负责提交作业配置并适配Java计算框架运行C++代码。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Submitter extends Configured implements Tool {

  protected static final Logger LOG = LoggerFactory.getLogger(Submitter.class);
  /** 是否保留调试用的命令文件配置项 */
  public static final String PRESERVE_COMMANDFILE = 
    "mapreduce.pipes.commandfile.preserve";
  /** 可执行程序URI配置项 */
  public static final String EXECUTABLE = "mapreduce.pipes.executable";
  /** 可执行程序解释器配置项 */
  public static final INTERPRETOR = 
    "mapreduce.pipes.executable.interpretor";
  /** Mapper是否为Java实现配置项 */
  public static final String IS_JAVA_MAP = "mapreduce.pipes.isjavamapper";
  /** RecordReader是否为Java实现配置项 */
  public static final String IS_JAVA_RR = "mapreduce.pipes.isjavarecordreader";
  /** RecordWriter是否为Java实现配置项 */
  public static final String IS_JAVA_RW = "mapreduce.pipes.isjavarecordwriter";
  /** Reducer是否为Java实现配置项 */
  public static final String IS_JAVA_REDUCE = "mapreduce.pipes.isjavareducer";
  /** 用户自定义Java分区类配置项 */
  public static final String PARTITIONER = "mapreduce.pipes.partitioner";
  /** 输入格式类配置项 */
  public static final String INPUT_FORMAT = "mapreduce.pipes.inputformat";
  /** 命令端口环境变量名（仅用于测试），环境变量不允许包含点号 */
  public static final String PORT = "mapreduce_pipes_command_port";
  
  /**
   * 空构造方法，使用默认配置初始化
   */
  public Submitter() {
    this(new Configuration());
  }
  
  /**
   * 使用指定配置初始化提交器
   * @param conf Hadoop配置对象
   */
  public Submitter(Configuration conf) {
    setConf(conf);
  }
  
  /**
   * 从配置中获取应用可执行程序的URI地址
   * @param conf 作业配置对象
   * @return 可执行程序的URI地址
   */
  public static String getExecutable(JobConf conf) {
    return conf.get(Submitter.EXECUTABLE);
  }

  /**
   * 设置应用可执行程序的URI地址，通常为HDFS上的路径
   * @param conf 作业配置对象
   * @param executable 可执行程序的URI地址
   */
  public static void setExecutable(JobConf conf, String executable) {
    conf.set(Submitter.EXECUTABLE, executable);
  }

  /**
   * 设置RecordReader是否由Java实现
   * @param conf 作业配置对象
   * @param value 是否为Java实现
   */
  public static void setIsJavaRecordReader(JobConf conf, boolean value) {
    conf.setBoolean(Submitter.IS_JAVA_RR, value);
  }

  /**
   * 获取RecordReader是否由Java实现
   * @param conf 作业配置对象
   * @return true表示RecordReader为Java实现
   */
  public static boolean getIsJavaRecordReader(JobConf conf) {
    return conf.getBoolean(Submitter.IS_JAVA_RR, false);
  }

  /**
   * 设置Mapper是否由Java实现
   * @param conf 作业配置对象
   * @param value 是否为Java实现
   */
  public static void setIsJavaMapper(JobConf conf, boolean value) {
    conf.setBoolean(Submitter.IS_JAVA_MAP, value);
  }

  /**
   * 获取Mapper是否由Java实现
   * @param conf 作业配置对象
   * @return true表示Mapper为Java实现
   */
  public static boolean getIsJavaMapper(JobConf conf) {
    return conf.getBoolean(Submitter.IS_JAVA_MAP, false);
  }

  /**
   * 设置Reducer是否由Java实现
   * @param conf 作业配置对象
   * @param value 是否为Java实现
   */
  public static void setIsJavaReducer(JobConf conf, boolean value) {
    conf.setBoolean(Submitter.IS_JAVA_REDUCE, value);
  }

  /**
   * 获取Reducer是否由Java实现
   * @param conf 作业配置对象
   * @return true表示Reducer为Java实现
   */
  public static boolean getIsJavaReducer(JobConf conf) {
    return conf.getBoolean(Submitter.IS_JAVA_REDUCE, false);
  }

  /**
   * 设置RecordWriter是否由Java实现
   * @param conf 作业配置对象
   * @param value 是否为Java实现
   */
  public static void setIsJavaRecordWriter(JobConf conf, boolean value) {
    conf.setBoolean(Submitter.IS_JAVA_RW, value);
  }

  /**
   * 获取RecordWriter是否由Java实现
   * @param conf 作业配置对象
   * @return true表示RecordWriter为Java实现
   */
  public static boolean getIsJavaRecordWriter(JobConf conf) {
    return conf.getBoolean(Submitter.IS_JAVA_RW, false);
  }

  /**
   * 仅当配置项未设置时，设置默认值
   * @param conf 作业配置对象
   * @param key 配置项键
   * @param value 默认值
   */
  private static void setIfUnset(JobConf conf, String key, String value) {
    if (conf.get(key) == null) {
      conf.set(key, value);
    }
  }

  /**
   * 保存用户自定义的原始Java分区类，供后续Pipes分区器使用
   * @param conf 作业配置对象
   * @param cls 用户自定义分区类
   */
  static void setJavaPartitioner(JobConf conf, Class cls) {
    conf.set(Submitter.PARTITIONER, cls.getName());
  }
  
  /**
   * 获取用户自定义的原始Java分区类
   * @param conf 作业配置对象
   * @return 用户自定义分区类，不存在则返回HashPartitioner
   */
  static Class<? extends Partitioner> getJavaPartitioner(JobConf conf) {
    return conf.getClass(Submitter.PARTITIONER, 
                         HashPartitioner.class,
                         Partitioner.class);
  }

  /**
   * 获取是否保留调试用命令文件，开启后会在任务目录生成downlink.data供调试使用
   * @param conf 作业配置对象
   * @return true表示保留命令文件用于调试
   */
  public static boolean getKeepCommandFile(JobConf conf) {
    return conf.getBoolean(Submitter.PRESERVE_COMMANDFILE, false);
  }

  /**
   * 设置是否保留调试用命令文件
   * @param conf 作业配置对象
   * @param keep 是否保留
   */
  public static void setKeepCommandFile(JobConf conf, boolean keep) {
    conf.setBoolean(Submitter.PRESERVE_COMMANDFILE, keep);
  }

  /**
   * 提交Pipes作业到MapReduce集群，会修改传入的作业配置对象
   * @param conf 作业配置对象
   * @return 运行中的作业句柄
   * @throws IOException 提交过程IO异常
   * @deprecated 使用 {@link Submitter#runJob(JobConf)} 替代
   */
  @Deprecated
  public static RunningJob submitJob(JobConf conf) throws IOException {
    return runJob(conf);
  }

  /**
   * 提交Pipes作业到MapReduce集群并运行，完成必要的Pipes配置修改
   * @param conf 作业配置对象（会被修改）
   * @return 运行中的作业句柄
   * @throws IOException 提交过程IO异常
   */
  public static RunningJob runJob(JobConf conf) throws IOException {
    setupPipesJob(conf);
    return JobClient.runJob(conf);
  }

  /**
   * 仅提交Pipes作业不等待完成，返回作业句柄供跟踪
   * @param conf 作业配置对象（会被修改）
   * @return 运行中的作业句柄
   * @throws IOException 提交过程IO异常
   */
  public static RunningJob jobSubmit(JobConf conf) throws IOException {
    setupPipesJob(conf);
    return new JobClient(conf).submitJob(conf);
  }
  
  /**
   * 为Pipes作业配置必要的框架参数，替换默认组件为Pipes适配实现
   * @param conf 作业配置对象
   * @throws IOException 配置过程IO异常
   */
  private static void setupPipesJob(JobConf conf) throws IOException {
    // 默认Map输出类型为Text
    if (!getIsJavaMapper(conf)) {
      // 使用Pipes的MapRunner适配C++ Mapper
      conf.setMapRunnerClass(PipesMapRunner.class);
      // 保存用户分区类，替换为Pipes分区器
      setJavaPartitioner(conf, conf.getPartitionerClass());
      conf.setPartitionerClass(PipesPartitioner.class);
    }
    if (!getIsJavaReducer(conf)) {
      // 使用Pipes的Reducer适配C++ Reducer
      conf.setReducerClass(PipesReducer.class);
      if (!getIsJavaRecordWriter(conf)) {
        // C++输出时使用NullOutputFormat，不生成Java输出文件
        conf.setOutputFormat(NullOutputFormat.class);
      }
    }
    String textClassname = Text.class.getName();
    // 未指定输出类型时默认使用Text
    setIfUnset(conf, MRJobConfig.MAP_OUTPUT_KEY_CLASS, textClassname);
    setIfUnset(conf, MRJobConfig.MAP_OUTPUT_VALUE_CLASS, textClassname);
    setIfUnset(conf, MRJobConfig.OUTPUT_KEY_CLASS, textClassname);
    setIfUnset(conf, MRJobConfig.OUTPUT_VALUE_CLASS, textClassname);
    
    // C++ RecordReader需要使用PipesNonJavaInputFormat处理进度上报
    if (!getIsJavaRecordReader(conf) && !getIsJavaMapper(conf)) {
      // 保存原始输入格式类
      conf.setClass(Submitter.INPUT_FORMAT, 
                    conf.getInputFormat().getClass(), InputFormat.class);
      // 替换为Pipes适配输入格式
      conf.setInputFormat(PipesNonJavaInputFormat.class);
    }
    
    String exec = getExecutable(conf);
    if (exec == null) {
      throw new IllegalArgumentException("No application program defined.");
    }
    // 可执行路径为<路径>#<程序名>格式时，添加默认gdb调试脚本
    if (exec.contains("#")) {
      String defScript = "$HADOOP_HOME/src/c++/pipes/debug/pipes-default-script";
      setIfUnset(conf, MRJobConfig.MAP_DEBUG_SCRIPT,defScript);
      setIfUnset(conf, MRJobConfig.REDUCE_DEBUG_SCRIPT,defScript);
    }
    // 将可执行程序添加到分布式缓存，分发到所有计算节点
    URI[] fileCache = JobContextImpl.getCacheFiles(conf);
    if (fileCache == null) {
      fileCache = new URI[1];
    } else {
      // 已有缓存文件时扩展数组长度
      URI[] tmp = new URI[fileCache.length+1];
      System.arraycopy(fileCache, 0, tmp, 1, fileCache.length);
      fileCache = tmp;
    }
    try {
      // 可执行程序放在缓存数组第一个位置
      fileCache[0] = new URI(exec);
    } catch (URISyntaxException e) {
      IOException ie = new IOException("Problem parsing execable URI " + exec);
      ie.initCause(e);
      throw ie;
    }
    Job.setCacheFiles(fileCache, conf);
  }

  /**
   * Pipes命令行参数解析器，处理CLI提交的参数
   */
  static class CommandLineParser {
    private Options options = new Options();
    
    /**
     * 添加命令行选项
     * @param longName 选项长名称
     * @param required 是否必填
     * @param description 选项描述
     * @param paramName 参数名称
     */
    void addOption(String longName, boolean required, String description, 
                   String paramName) {
      Option option = Option.builder(longName).argName(paramName)
          .hasArg().desc(description).required(required).build();
      options.addOption(option);
    }
    
    /**
     * 添加命令行参数
     * @param name 参数名称
     * @param required 是否必填
     * @param description 参数描述
     */
    void addArgument(String name, boolean required, String description) {
      Option option = Option.builder().argName(name)
          .hasArg().desc(description).required(required).build();
      options.addOption(option);

    }

    /**
     * 创建CLI解析器实例
     * @return 基础解析器实例
     */
    Parser createParser() {
      Parser result = new BasicParser();
      return result;
    }
    
    /**
     * 打印命令行使用帮助
     */
    void printUsage() {
      System.out.println("Usage: pipes ");
      System.out.println("  [-input <path>] // Input directory");
      System.out.println("  [-output <path>] // Output directory");
      System.out.println("  [-jar <jar file> // jar filename");
      System.out.println("  [-inputformat <class>] // InputFormat class");
      System.out.println("  [-map <class>] // Java Map class");
      System.out.println("  [-partitioner <class>] // Java Partitioner");
      System.out.println("  [-reduce <class>] // Java Reduce class");
      System.out.println("  [-writer <class>] // Java RecordWriter");
      System.out.println("  [-program <executable>] // executable URI");
      System.out.println("  [-reduces <num>] // number of reduces");
      System.out.println("  [-lazyOutput <true/false>] // createOutputLazily");
      System.out.println();
      GenericOptionsParser.printGenericCommandUsage(System.out);
    }
  }
  
  /**
   * 从命令行参数中加载指定类型的类
   * @param cl 命令行解析结果
   * @param key 参数名
   * @param conf 作业配置
   * @param cls 期望的接口类型
   * @return 加载后的类对象
   * @throws ClassNotFoundException 类找不到异常
   */
  private static <InterfaceType> 
  Class<? extends InterfaceType> getClass(CommandLine cl, String key, 
                                          JobConf conf, 
                                          Class<InterfaceType> cls
                                         ) throws ClassNotFoundException {
    return conf.getClassByName(cl.getOptionValue(key)).asSubclass(cls);
  }