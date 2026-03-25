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

package org.apache.hadoop.mapreduce.task;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * JobContext接口的实现类，为运行中的任务提供作业配置的只读视图。
 * 任务执行过程中通过该类获取作业全局配置信息，不允许修改作业配置。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobContextImpl implements JobContext {

  protected final org.apache.hadoop.mapred.JobConf conf;
  private JobID jobId;
  /**
   * 当前提交作业用户的用户信息对象
   */
  protected UserGroupInformation ugi;
  protected final Credentials;
  
  /**
   * 构造JobContextImpl实例，基于给定配置和作业ID初始化作业上下文。
   * @param conf 作业配置对象
   * @param jobId 作业唯一ID
   */
  public JobContextImpl(Configuration conf, JobID jobId) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf)conf;
    } else {
      this.conf = new JobConf(conf);
    }
    this.jobId = jobId;
    this.credentials = this.conf.getCredentials();
    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 获取作业的配置对象。
   * @return 作业共享配置对象
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * 获取作业的唯一ID。
   * @return 作业ID对象
   */
  public JobID getJobID() {
    return jobId;
  }
  
  /**
   * 设置作业ID。
   * @param jobId 要设置的作业ID
   */
  public void setJobID(JobID jobId) {
    this.jobId = jobId;
  }
  
  /**
   * 获取作业配置的Reduce任务数量，默认值为1。
   * @return Reduce任务数量
   */
  public int getNumReduceTasks() {
    return conf.getNumReduceTasks();
  }
  
  /**
   * 获取默认文件系统的当前工作目录。
   * @return 工作目录路径
   * @throws IOException 获取目录失败时抛出IO异常
   */
  public Path getWorkingDirectory() throws IOException {
    return conf.getWorkingDirectory();
  }

  /**
   * 获取作业最终输出的Key类型Class。
   * @return 输出Key类对象
   */
  public Class<?> getOutputKeyClass() {
    return conf.getOutputKeyClass();
  }
  
  /**
   * 获取作业最终输出的Value类型Class。
   * @return 输出Value类对象
   */
  public Class<?> getOutputValueClass() {
    return conf.getOutputValueClass();
  }

  /**
   * 获取Map阶段输出的Key类型Class，如果未配置则使用最终输出Key类型。
   * @return Map输出Key类对象
   */
  public Class<?> getMapOutputKeyClass() {
    return conf.getMapOutputKeyClass();
  }

  /**
   * 获取Map阶段输出的Value类型Class，如果未配置则使用最终输出Value类型。
   * @return Map输出Value类对象
   */
  public Class<?> getMapOutputValueClass() {
    return conf.getMapOutputValueClass();
  }

  /**
   * 获取用户指定的作业名称，仅用于展示标识。
   * @return 作业名称，默认为空字符串
   */
  public String getJobName() {
    return conf.getJobName();
  }

  /**
   * 获取作业配置的InputFormat实现类。
   * @return InputFormat类对象，默认返回TextInputFormat
   * @throws ClassNotFoundException 类找不到时抛出异常
   */
  @SuppressWarnings("unchecked")
  public Class<? extends InputFormat<?,?>> getInputFormatClass() 
     throws ClassNotFoundException {
    return (Class<? extends InputFormat<?,?>>) 
      conf.getClass(INPUT_FORMAT_CLASS_ATTR, TextInputFormat.class);
  }

  /**
   * 获取作业配置的Mapper实现类。
   * @return Mapper类对象，默认返回Mapper基类
   * @throws ClassNotFoundException 类找不到时抛出异常
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Mapper<?,?,?,?>> getMapperClass() 
     throws ClassNotFoundException {
    return (Class<? extends Mapper<?,?,?,?>>) 
      conf.getClass(MAP_CLASS_ATTR, Mapper.class);
  }

  /**
   * 获取作业配置的Combiner实现类。
   * @return Combiner类对象，未配置则返回null
   * @throws ClassNotFoundException 类找不到时抛出异常
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getCombinerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Reducer<?,?,?,?>>) 
      conf.getClass(COMBINE_CLASS_ATTR, null);
  }

  /**
   * 获取作业配置的Reducer实现类。
   * @return Reducer类对象，默认返回Reducer基类
   * @throws ClassNotFoundException 类找不到时抛出异常
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getReducerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Reducer<?,?,?,?>>) 
      conf.getClass(REDUCE_CLASS_ATTR, Reducer.class);
  }

  /**
   * 获取作业配置的OutputFormat实现类。
   * @return OutputFormat类对象，默认返回TextOutputFormat
   * @throws ClassNotFoundException 类找不到时抛出异常
   */
  @SuppressWarnings("unchecked")
  public Class<? extends OutputFormat<?,?>> getOutputFormatClass() 
     throws ClassNotFoundException {
    return (Class<? extends OutputFormat<?,?>>) 
      conf.getClass(OUTPUT_FORMAT_CLASS_ATTR, TextOutputFormat.class);
  }

  /**
   * 获取作业配置的Partitioner实现类。
   * @return Partitioner类对象，默认返回HashPartitioner
   * @throws ClassNotFoundException 类找不到时抛出异常
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Partitioner<?,?>> getPartitionerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Partitioner<?,?>>) 
      conf.getClass(PARTITIONER_CLASS_ATTR, HashPartitioner.class);
  }

  /**
   * 获取用于排序Map输出Key的比较器。
   * @return Key排序比较器
   */
  public RawComparator<?> getSortComparator() {
    return conf.getOutputKeyComparator();
  }

  /**
   * 获取作业Jar包的路径。
   * @return Jar包路径字符串
   */
  public String getJar() {
    return conf.getJar();
  }

  /**
   * 获取用户定义的Combiner分组Key比较器，用于对输入到Combiner的Key进行分组。
   * @return Combiner分组Key比较器
   */
  public RawComparator<?> getCombinerKeyGroupingComparator() {
    return conf.getCombinerKeyGroupingComparator();
  }

  /** 
   * 获取用户定义的Reduce分组Key比较器，用于对输入到Reduce的Key进行分组。
   * @return Reduce分组Key比较器
   */
  public RawComparator<?> getGroupingComparator() {
    return conf.getOutputValueGroupingComparator();
  }
  
  /**
   * 获取作业是否需要执行作业级别的设置和清理。
   * @return true表示需要执行，false表示不需要
   */
  public boolean getJobSetupCleanupNeeded() {
    return conf.getBoolean(MRJobConfig.SETUP_CLEANUP_NEEDED, true);
  }
  
  /**
   * 获取作业是否需要执行任务级别的清理。
   * @return true表示需要清理，false表示不需要
   */
  public boolean getTaskCleanupNeeded() {
    return conf.getBoolean(MRJobConfig.TASK_CLEANUP_NEEDED, true);
  }

  /**
   * 检查是否需要为工作目录中的本地化缓存文件创建符号链接。
   * @return true表示需要创建符号链接，false表示不需要
   */
  public boolean getSymlink() {
    return DistributedCache.getSymlink(conf);
  }
  
  /**
   * 获取类路径中归档条目对应的路径数组。
   * @return 归档条目路径数组
   */
  public Path[] getArchiveClassPaths() {
    return getArchiveClassPaths(conf);
  }

  /**
   * 从给定配置中解析出类路径中的归档条目路径数组，供DistributedCache内部使用。
   * @param conf 包含类路径配置的配置对象
   * @return 类路径中归档条目组成的Path数组，无条目则返回null
   */
  public static Path[] getArchiveClassPaths(Configuration conf) {
    ArrayList<String> list = (ArrayList<String>)conf.getStringCollection(
        MRJobConfig.CLASSPATH_ARCHIVES);
    if (list.size() == 0) {
      return null;
    }
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path(list.get(i));
    }
    return paths;
  }

  /**
   * 获取配置中设置的分布式缓存归档文件URI数组。
   * @return 缓存归档文件URI数组
   * @throws IOException 解析失败时抛出IO异常
   */
  public URI[] getCacheArchives() throws IOException {
    return getCacheArchives(conf);
  }

  /**
   * 从给定配置中解析出分布式缓存归档文件URI数组，供内部代码使用。
   * @param conf 包含缓存配置的配置对象
   * @return 缓存归档文件URI数组
   */
  public static URI[] getCacheArchives(Configuration conf) {
    return StringUtils.stringToURI(conf.getStrings(MRJobConfig.CACHE_ARCHIVES));
  }

  /**
   * 获取配置中设置的分布式缓存普通文件URI数组。
   * @return 缓存普通文件URI数组
   * @throws IOException 解析失败时抛出IO异常
   */
  public URI[] getCacheFiles() throws IOException {
    return getCacheFiles(conf);
  }

  /**
   * 从给定配置中解析出分布式缓存普通文件URI数组，供内部代码使用。
   * @param conf 包含缓存配置的配置对象
   * @return 缓存普通文件URI数组
   */
  public static URI[] getCacheFiles(Configuration conf) {
    return StringUtils.stringToURI(conf.getStrings(MRJobConfig.CACHE_FILES));
  }

  /**
   * 获取本地化后缓存归档文件的路径数组。
   * @return 本地化归档文件路径数组
   * @throws IOException 解析失败时抛出IO异常
   */
  public Path[] getLocalCacheArchives()
    throws IOException {
    return getLocalCacheArchives(conf);
  }

  /**
   * 从给定配置中解析出本地化后缓存归档文件的路径数组，供内部代码使用。
   * @param conf 包含本地化缓存配置的配置对象
   * @return 本地化归档文件路径数组
   */
  public static Path[] getLocalCacheArchives(Configuration conf) {
    return StringUtils.stringToPath(conf.getStrings(MRJobConfig.CACHE_LOCALARCHIVES));
  }

  /**
   * 获取本地化后缓存普通文件的路径数组。
   * @return 本地化普通文件路径数组
   * @throws IOException 解析失败时抛出IO异常
   */
  public Path[] getLocalCacheFiles()
    throws IOException {
    return getLocalCacheFiles(conf);
  }

  /**
   * 从给定配置中解析出本地化后缓存普通文件的路径数组，供内部代码使用。
   * @param conf 包含本地化缓存配置的配置对象
   * @return 本地化普通文件路径数组
   */
  public static Path[] getLocalCacheFiles(Configuration conf) {
    return StringUtils.stringToPath(conf.getStrings(MRJobConfig.CACHE_LOCALFILES));
  }

  /**
   * 将字符串数组解析为时间戳long数组。
   * @param strs 待解析的字符串数组
   * @return 解析后的时间戳数组，长度与输入数组一致，输入为null则返回null
   */
  private static long[] parseTimestamps(String[] strs) {
    if (strs == null) {
      return null;
    }
    long[] result = new long[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Long.parseLong(strs[i]);
    }
    return result;
  }

  /**
   * 从给定配置中获取缓存归档文件的时间戳数组，供内部代码使用。
   * @param conf 存储时间戳配置的配置对象
   * @return 归档文件时间戳数组
   */
  public static long[] getArchiveTimestamps(Configuration conf) {
    return parseTimestamps(conf.getStrings(MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS));
  }

  /**
   * 从给定配置中获取缓存普通文件的时间戳数组，供内部代码使用。
   * @param conf 存储时间戳配置的配置对象
   * @return 普通文件时间戳数组
   */
  public static long[] getFileTimestamps(Configuration conf) {
    return parseTimestamps(conf.getStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS));
  }

  /**
   * 获取类路径中普通文件条目对应的路径数组。
   * @return 普通文件条目路径数组
   */
  public Path[] getFileClassPaths() {
    return getFileClassPaths(conf);
  }

  /**
   * 从给定配置中解析出类路径中的普通文件条目路径数组，供DistributedCache内部使用。
   * @param conf 包含类路径配置的配置对象
   * @return 类路径中普通文件条目组成的Path数组，无条目则返回null
   */
  public static Path[] getFileClassPaths(Configuration conf) {
    ArrayList<String> list =
        (ArrayList<String>) conf.getStringCollection(MRJobConfig.CLASSPATH_FILES);
    if (list.size() == 0) {
      return null;
    }
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path(list.get(i));
    }
    return paths;
  }

  /**
   * 将时间戳long数组转换为字符串数组。
   * @param timestamps 待转换的时间戳long数组
   * @return 转换后的字符串数组，长度与输入一致，输入为null则返回null
   */
  private static String[] toTimestampStrs(long[] timestamps) {
    if (timestamps == null) {
      return null;
    }
    String[] result = new String[timestamps.length];
    for(int i=0; i < timestamps.length; ++i) {
      result[i] = Long.toString(timestamps[i]);
    }
    return result;
  }

  /**
   * 获取缓存归档文件的时间戳字符串数组，供内部代码使用。
   * @return 归档文件时间戳字符串数组
   */
  public String[] getArchiveTimestamps() {
    return toTimestampStrs(getArchiveTimestamps(conf));
  }

  /**
   * 获取缓存普通文件的时间戳字符串数组，供内部代码使用。
   * @return 普通文件时间戳字符串数组
   */
  public String[] getFileTimestamps() {
    return toTimestampStrs(getFileTimestamps(conf));
  }

  /** 
   * 获取