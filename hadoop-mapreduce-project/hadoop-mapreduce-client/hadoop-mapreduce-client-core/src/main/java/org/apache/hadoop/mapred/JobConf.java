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

package org.apache.hadoop.mapred;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * MapReduce旧API（MR1）的作业配置类，用于描述一个MapReduce作业的全部参数，是用户向Hadoop框架提交作业配置的主要接口。
 * 
 * <p><code>JobConf</code> 会保存作业的输入输出路径、Mapper/Reducer实现类、分区器、排序比较器、任务数量、资源需求等全部作业运行参数，
 * 框架会根据JobConf中配置的参数忠实执行作业。但存在两种例外情况：
 * <ol>
 *   <li>
 *   部分配置参数可能被管理员标记为final不可修改，用户修改不会生效。
 *   </li>
 *   <li>
 *   部分参数（如{@link #setNumMapTasks(int)}）仅作为框架的提示，实际作业执行时会根据输入数据大小自动调整。
 *   </li>
 * </ol>
 * 
 * <p>典型的JobConf会指定{@link Mapper}、可选Combiner、{@link Partitioner}、{@link Reducer}、
 * {@link InputFormat}和{@link OutputFormat}的具体实现类，同时也可以配置高级特性如自定义比较器、
 * 分布式缓存文件、中间结果压缩、失败任务调试脚本等。</p>
 * 
 * <p>使用示例：</p>
 * <p><blockquote><pre>
 *     // 创建新的JobConf
 *     JobConf job = new JobConf(new Configuration(), MyJob.class);
 *     
 *     // 指定作业参数
 *     job.setJobName("myjob");
 *     
 *     FileInputFormat.setInputPaths(job, new Path("in"));
 *     FileOutputFormat.setOutputPath(job, new Path("out"));
 *     
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setCombinerClass(MyJob.MyReducer.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 *     
 *     job.setInputFormat(SequenceFileInputFormat.class);
 *     job.setOutputFormat(SequenceFileOutputFormat.class);
 * </pre></blockquote>
 * 
 * @see JobClient
 * @see ClusterStatus
 * @see Tool
 * @see DistributedCache
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JobConf extends Configuration {

  private static final Logger LOG = LoggerFactory.getLogger(JobConf.class);
  /** 用于匹配Javaopts中的-Xmx参数的正则表达式 */
  private static final Pattern JAVA_OPTS_XMX_PATTERN =
          Pattern.compile(".*(?:^|\\s)-Xmx(\\d+)([gGmMkK]?)(?:$|\\s).*");

  /** 静态加载MapReduce默认配置资源 */
  static{
    ConfigUtil.loadResources();
  }

  /**
   * @deprecated Use {@link #MAPREDUCE_JOB_MAP_MEMORY_MB_PROPERTY} and
   * {@link #MAPREDUCE_JOB_REDUCE_MEMORY_MB_PROPERTY}
   */
  @Deprecated
  public static final String MAPRED_TASK_MAXVMEM_PROPERTY =
    "mapred.task.maxvmem";

  /**
   * @deprecated 
   */
  @Deprecated
  public static final String UPPER_LIMIT_ON_TASK_VMEM_PROPERTY =
    "mapred.task.limit.maxvmem";

  /**
   * @deprecated
   */
  @Deprecated
  public static final String MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY =
    "mapred.task.default.maxvmem";

  /**
   * @deprecated
   */
  @Deprecated
  public static final String MAPRED_TASK_MAXPMEM_PROPERTY =
    "mapred.task.maxpmem";

  /**
   * 内存配置选项中表示禁用内存限制的标记值，在MR2中已废弃不再使用。
   */
  @Deprecated
  public static final long DISABLED_MEMORY_LIMIT = -1L;

  /**
   * MapReduce集群本地目录配置项名称
   */
  public static final String MAPRED_LOCAL_DIR_PROPERTY = MRConfig.LOCAL_DIR;

  /**
   * 作业未指定队列时使用的默认提交队列名称
   */
  public static final String DEFAULT_QUEUE_NAME = "default";

  static final String MAPREDUCE_JOB_MAP_MEMORY_MB_PROPERTY =
      JobContext.MAP_MEMORY_MB;

  static final String MAPREDUCE_JOB_REDUCE_MEMORY_MB_PROPERTY =
    JobContext.REDUCE_MEMORY_MB;

  /**
   * 为兼容MR1应用保留的配置项，MR2应用应使用{@link #MAPREDUCE_JOB_MAP_MEMORY_MB_PROPERTY}
   */
  @Deprecated
  public static final String MAPRED_JOB_MAP_MEMORY_MB_PROPERTY =
      "mapred.job.map.memory.mb";

  /**
   * 为兼容MR1应用保留的配置项，MR2应用应使用{@link #MAPREDUCE_JOB_REDUCE_MEMORY_MB_PROPERTY}
   */
  @Deprecated
  public static final String MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY =
      "mapred.job.reduce.memory.mb";

  /** 作业JAR默认解压规则：解压classes/和lib/目录下所有内容 */
  public static final Pattern UNPACK_JAR_PATTERN_DEFAULT =
    Pattern.compile("(?:classes/|lib/).*");

  /**
   * 配置项：Map和Reduce任务通用的JVM启动参数，已废弃，请分别为Map和Reduce单独配置。
   * 
   * @deprecated Use {@link #MAPRED_MAP_TASK_JAVA_OPTS} or 
   *                 {@link #MAPRED_REDUCE_TASK_JAVA_OPTS}
   */
  @Deprecated
  public static final String MAPRED_TASK_JAVA_OPTS = "mapred.child.java.opts";
  
  /**
   * 配置项：Map任务的JVM启动参数，支持@taskid@占位符会被替换为当前任务ID。
   */
  public static final String MAPRED_MAP_TASK_JAVA_OPTS = 
    JobContext.MAP_JAVA_OPTS;
  
  /**
   * 配置项：Reduce任务的JVM启动参数，支持@taskid@占位符会被替换为当前任务ID。
   */
  public static final String MAPRED_REDUCE_TASK_JAVA_OPTS = 
    JobContext.REDUCE_JAVA_OPTS;

  public static final String DEFAULT_MAPRED_TASK_JAVA_OPTS = "";

  /**
   * @deprecated
   * 配置项：任务最大虚拟内存ulimit限制，已废弃不再生效。
   */
  @Deprecated
  public static final String MAPRED_TASK_ULIMIT = "mapred.child.ulimit";

  /**
   * @deprecated
   * 配置项：Map任务最大虚拟内存ulimit限制，已废弃不再生效。
   */
  @Deprecated
  public static final String MAPRED_MAP_TASK_ULIMIT = "mapreduce.map.ulimit";
  
  /**
   * @deprecated
   * 配置项：Reduce任务最大虚拟内存ulimit限制，已废弃不再生效。
   */
  @Deprecated
  public static final String MAPRED_REDUCE_TASK_ULIMIT =
    "mapreduce.reduce.ulimit";


  /**
   * 配置项：Map和Reduce任务通用的环境变量，已废弃，请分别为Map和Reduce单独配置。
   * 
   * @deprecated Use {@link #MAPRED_MAP_TASK_ENV} or 
   *                 {@link #MAPRED_REDUCE_TASK_ENV}
   */
  @Deprecated
  public static final String MAPRED_TASK_ENV = "mapred.child.env";

  /**
   * 配置项：Map任务的环境变量，格式为k1=v1,k2=v2，支持引用已有环境变量$key(Linux)/%key%(Windows)。
   * 也可以通过mapreduce.map.env.VARNAME=value方式单独添加环境变量。
   */
  public static final String MAPRED_MAP_TASK_ENV = JobContext.MAP_ENV;
  
  /**
   * 配置项：Reduce任务的环境变量，格式为k1=v1,k2=v2，支持引用已有环境变量$key(Linux)/%key%(Windows)。
   * 也可以通过mapreduce.reduce.env.VARNAME=value方式单独添加环境变量。
   */
  public static final String MAPRED_REDUCE_TASK_ENV = JobContext.REDUCE_ENV;

  /** 作业凭证信息，用于安全认证 */
  private Credentials credentials = new Credentials();
  
  /**
   * 配置项：Map任务日志级别，支持OFF/FATAL/ERROR/WARN/INFO/DEBUG/TRACE/ALL。
   */
  public static final String MAPRED_MAP_TASK_LOG_LEVEL = 
    JobContext.MAP_LOG_LEVEL;
  
  /**
   * 配置项：Reduce任务日志级别，支持OFF/FATAL/ERROR/WARN/INFO/DEBUG/TRACE/ALL。
   */
  public static final String MAPRED_REDUCE_TASK_LOG_LEVEL = 
    JobContext.REDUCE_LOG_LEVEL;
  
  /**
   * Map/Reduce任务默认日志级别
   */
  public static final String DEFAULT_LOG_LEVEL = JobContext.DEFAULT_LOG_LEVEL;

  /**
   * 为兼容MR1应用保留的配置项，MR2应用应使用{@link MRJobConfig#WORKFLOW_ID} instead
   */
  @Deprecated
  public static final String WORKFLOW_ID = MRJobConfig.WORKFLOW_ID;

  /**
   * 为兼容MR1应用保留的配置项，MR2应用应使用{@link MRJobConfig#WORKFLOW_NAME} instead
   */
  @Deprecated
  public static final String WORKFLOW_NAME = MRJobConfig.WORKFLOW_NAME;

  /**
   * 为兼容MR1应用保留的配置项，MR2应用应使用{@link MRJobConfig#WORKFLOW_NODE_NAME} instead
   */
  @Deprecated
  public static final String WORKFLOW_NODE_NAME =
      MRJobConfig.WORKFLOW_NODE_NAME;

  /**
   * 为兼容MR1应用保留的配置项，MR2应用应使用{@link MRJobConfig#WORKFLOW_ADJACENCY_PREFIX_STRING} instead
   */
  @Deprecated
  public static final String WORKFLOW_ADJACENCY_PREFIX_STRING =
      MRJobConfig.WORKFLOW_ADJACENCY_PREFIX_STRING;

  /**
   * 为兼容MR1应用保留的配置项，MR2应用应使用{@link MRJobConfig#WORKFLOW_ADJACENCY_PREFIX_PATTERN} instead
   */
  @Deprecated
  public static final String WORKFLOW_ADJACENCY_PREFIX_PATTERN =
      MRJobConfig.WORKFLOW_ADJACENCY_PREFIX_PATTERN;

  /**
   * 为兼容MR1应用保留的配置项，MR2应用应使用{@link MRJobConfig#WORKFLOW_TAGS} instead
   */
  @Deprecated
  public static final String WORKFLOW_TAGS = MRJobConfig.WORKFLOW_TAGS;

  /**
   * 为兼容MR1应用保留的配置项，MR2应用不应再使用。
   */
  @Deprecated
  public static final String MAPREDUCE_RECOVER_JOB =
      "mapreduce.job.restart.recover";

  /**
   * 为兼容MR1应用保留的配置项默认值，MR2应用不应再使用。
   */
  @Deprecated
  public static final boolean DEFAULT_MAPREDUCE_RECOVER_JOB = true;

  /**
   * 构造一个空的MapReduce作业配置对象。
   */
  public JobConf() {
    checkAndWarnDeprecation();
  }

  /** 
   * 构造一个MapReduce作业配置对象，自动从示例类所在Jar包设置作业Jar路径。
   * 
   * @param exampleClass 示例类，框架会自动找到该类所在Jar作为作业Jar
   */
  public JobConf(Class exampleClass) {
    setJarByClass(exampleClass);
    checkAndWarnDeprecation();
  }
  
  /**
   * 构造一个MapReduce作业配置对象，继承已有配置对象的配置。
   * 
   * @param conf 基础配置对象，会继承其中所有配置项
   */
  public JobConf(Configuration conf) {
    super(conf);
    
    if (conf instanceof JobConf) {
      JobConf that = (JobConf)conf;
      credentials = that.credentials;
    }
    
    checkAndWarnDeprecation();
  }


  /** 
   * 构造一个MapReduce作业配置对象，继承已有配置并自动从示例类所在Jar包设置作业Jar路径。
   * 
   * @param conf 基础配置对象，会继承其中所有配置项
   * @param exampleClass 示例类，框架会自动找到该类所在Jar作为作业Jar
   */
  public JobConf(Configuration conf, Class exampleClass) {
    this(conf);
    setJarByClass(exampleClass);
  }


  /** 
   * 构造一个MapReduce作业配置对象，从指定XML配置文件加载配置。
   *
   * @param config XML配置文件路径
   */
  public JobConf(String config) {
    this(new Path(config));
  }

  /** 
   * 构造一个MapReduce作业配置对象，从指定XML配置文件路径加载配置。
   *
   * @param config XML配置文件路径
   */
  public JobConf(Path config) {
    super();
    addResource(config);
    checkAndWarnDeprecation();
  }

  /** 
   * 构造一个MapReduce作业配置对象，可指定是否加载默认配置资源。
   *
   * @param loadDefaults 是否加载默认配置资源
   */
  public JobConf(boolean loadDefaults) {
    super(loadDefaults);
    checkAndWarnDeprecation();
  }

  /**
   * 获取作业的安全凭证信息。
   * @return 作业凭证对象
   */
  public Credentials getCredentials() {
    return credentials;
  }
  
  @Private
  public void setCredentials(Credentials credentials) {
    this.credentials = credentials;
  }
  
  /**
   * 获取作业用户Jar包路径。
   * 
   * @return 作业用户Jar包路径
   */
  public String getJar() { return get(JobContext.JAR); }
  
  /**
   * 设置作业用户Jar包路径。
   * 
   * @param jar 作业用户Jar包路径
   */
  public void setJar(String jar) { set(JobContext.JAR, jar); }

  /**
   * 获取作业Jar需要解压到TaskTracker的内容匹配模式。
   * @return 解压内容匹配模式
   */
  public Pattern getJarUnpackPattern() {
    return getPattern(JobContext.JAR_UNPACK_PATTERN, UNPACK_JAR_PATTERN_DEFAULT);
  }

  
  /**
   * 通过示例类自动设置作业Jar文件路径，会自动找到该类所在Jar包。
   * 
   * @param cls 示例类，作业中用户自定义的业务类
   */
  public void setJarByClass(Class cls) {
    String jar = ClassUtil.findContainingJar(cls);
    if (jar != null) {
      setJar(jar);
    }   
  }

  /**
   * 获取MapReduce集群本地目录列表。
   * @return 本地目录数组
   * @throws IOException 获取配置失败时抛出异常
   */
  public String[] getLocalDir