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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.Credentials;

/**
 * 文件职责：MapReduce作业上下文只读接口，为运行中的任务提供作业配置信息的只读访问能力
 * 
 * 提供作业运行时所需的所有配置参数的只读查询，不允许修改作业配置，保证任务执行过程中配置一致性
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface JobContext extends MRJobConfig {
  /**
   * 获取作业的配置对象
   * @return 作业共享的配置对象
   */
  public Configuration getConfiguration();

  /**
   * 获取作业的安全凭证
   * @return 作业的安全凭证
   */
  public Credentials getCredentials();

  /**
   * 获取作业的唯一ID
   * @return 作业ID对象
   */
  public JobID getJobID();
  
  /**
   * 获取作业配置的Reduce任务数量，默认值为1
   * @return 作业的Reduce任务数量
   */
  public int getNumReduceTasks();
  
  /**
   * 获取默认文件系统上作业的当前工作目录
   * 
   * @return 工作目录路径
   * @throws IOException 获取目录失败时抛出IO异常
   */
  public Path getWorkingDirectory() throws IOException;

  /**
   * 获取作业输出数据的Key类型
   * @return 作业输出Key类型
   */
  public Class<?> getOutputKeyClass();
  
  /**
   * 获取作业输出数据的Value类型
   * @return 作业输出Value类型
   */
  public Class<?> getOutputValueClass();

  /**
   * 获取Map阶段输出数据的Key类型，如果未单独设置则使用最终输出Key类型
   * 允许Map输出Key类型与最终输出Key类型不同
   * @return Map输出Key类型
   */
  public Class<?> getMapOutputKeyClass();

  /**
   * 获取Map阶段输出数据的Value类型，如果未单独设置则使用最终输出Value类型
   * 允许Map输出Value类型与最终输出Value类型不同
   *  
   * @return Map输出Value类型
   */
  public Class<?> getMapOutputValueClass();

  /**
   * 获取用户指定的作业名称，仅用于用户识别作业
   * 
   * @return 作业名称，默认为空字符串
   */
  public String getJobName();

  /**
   * 获取作业配置的InputFormat类型
   * 
   * @return 作业配置的InputFormat类型
   * @throws ClassNotFoundException 找不到类时抛出异常
   */
  public Class<? extends InputFormat<?,?>> getInputFormatClass() 
     throws ClassNotFoundException;

  /**
   * 获取作业配置的Mapper类型
   * 
   * @return 作业配置的Mapper类型
   * @throws ClassNotFoundException 找不到类时抛出异常
   */
  public Class<? extends Mapper<?,?,?,?>> getMapperClass() 
     throws ClassNotFoundException;

  /**
   * 获取作业配置的Combiner类型
   * 
   * @return 作业配置的Combiner类型
   * @throws ClassNotFoundException 找不到类时抛出异常
   */
  public Class<? extends Reducer<?,?,?,?>> getCombinerClass() 
     throws ClassNotFoundException;

  /**
   * 获取作业配置的Reducer类型
   * 
   * @return 作业配置的Reducer类型
   * @throws ClassNotFoundException 找不到类时抛出异常
   */
  public Class<? extends Reducer<?,?,?,?>> getReducerClass() 
     throws ClassNotFoundException;

  /**
   * 获取作业配置的OutputFormat类型
   * 
   * @return 作业配置的OutputFormat类型
   * @throws ClassNotFoundException 找不到类时抛出异常
   */
  public Class<? extends OutputFormat<?,?>> getOutputFormatClass() 
     throws ClassNotFoundException;

  /**
   * 获取作业配置的Partitioner类型
   * 
   * @return 作业配置的Partitioner类型
   * @throws ClassNotFoundException 找不到类时抛出异常
   */
  public Class<? extends Partitioner<?,?>> getPartitionerClass() 
     throws ClassNotFoundException;

  /**
   * 获取用于Key排序的比较器
   * 
   * @return 用于Key比较的RawComparator实例
   */
  public RawComparator<?> getSortComparator();

  /**
   * 获取作业Jar包的路径
   * @return 作业Jar包路径
   */
  public String getJar();

  /**
   * 获取用户定义的Combiner输入Key分组比较器
   *
   * @return 用户设置的分组比较器
   * @see Job#setCombinerKeyGroupingComparatorClass(Class)
   */
  public RawComparator<?> getCombinerKeyGroupingComparator();

    /**
     * 获取用户定义的Reduce输入Key分组比较器
     *
     * @return 用户设置的分组比较器
     * @see Job#setGroupingComparatorClass(Class)
     * @see #getCombinerKeyGroupingComparator()
     */
  public RawComparator<?> getGroupingComparator();
  
  /**
   * 获取作业是否需要执行作业级别的设置和清理
   * 
   * @return 是否需要作业设置清理
   */
  public boolean getJobSetupCleanupNeeded();
  
  /**
   * 获取作业是否需要执行任务级别的清理
   * 
   * @return 是否需要任务清理
   */
  public boolean getTaskCleanupNeeded();

  /**
   * 获取任务性能分析是否启用
   * @return true 如果有任务需要进行性能分析
   */
  public boolean getProfileEnabled();

  /**
   * 获取性能分析器的配置参数
   *
   * 默认参数为 "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s"
   * 
   * @return 传递给任务子进程的性能分析配置参数
   */
  public String getProfileParams();

  /**
   * 获取需要进行性能分析的Map/Reduce任务范围
   * @param isMap 是否为Map任务
   * @return 需要性能分析的任务范围
   */
  public IntegerRanges getProfileTaskRange(boolean isMap);

  /**
   * 获取提交作业的用户名
   * 
   * @return 用户名
   */
  public String getUser();
  
  /**
   * 原本用于检查是否需要使用符号链接，但目前符号链接无法禁用
   * @return 始终返回true
   */
  @Deprecated
  public boolean getSymlink();
  
  /**
   * 获取类路径中归档条目对应的路径数组
   * @return 类路径归档路径数组
   */
  public Path[] getArchiveClassPaths();

  /**
   * 获取配置中设置的缓存归档文件URI数组
   * @return 配置中缓存归档文件的URI数组
   * @throws IOException 获取失败时抛出IO异常
   */
  public URI[] getCacheArchives() throws IOException;

  /**
   * 获取配置中设置的缓存文件URI数组
   * @return 配置中缓存文件的URI数组
   * @throws IOException 获取失败时抛出IO异常
   */
  public URI[] getCacheFiles() throws IOException;

  /**
   * 返回本地化缓存归档的路径数组
   * @return 本地化缓存归档路径数组
   * @throws IOException 获取失败时抛出IO异常
   * @deprecated 返回的数组仅包含已下载的条目，无法与{@link #getCacheArchives()}返回结果建立映射，已废弃
   */
  @Deprecated
  public Path[] getLocalCacheArchives() throws IOException;

  /**
   * 返回本地化缓存文件的路径数组
   * @return 本地化缓存文件路径数组
   * @throws IOException 获取失败时抛出IO异常
   * @deprecated 返回的数组仅包含已下载的条目，无法与{@link #getCacheFiles()}返回结果建立映射，已废弃
   */
  @Deprecated
  public Path[] getLocalCacheFiles() throws IOException;

  /**
   * 获取类路径中文件条目对应的路径数组
   * @return 类路径文件路径数组
   */
  public Path[] getFileClassPaths();
  
  /**
   * 获取缓存归档文件的时间戳，供分布式缓存和MapReduce内部代码使用
   * @return 时间戳字符串数组
   */
  public String[] getArchiveTimestamps();

  /**
   * 获取缓存文件的时间戳，供分布式缓存和MapReduce内部代码使用
   * @return 时间戳字符串数组
   */
  public String[] getFileTimestamps();

  /** 
   * 获取配置的Map任务最大尝试次数，对应配置项<code>mapred.map.max.attempts</code>
   * 如果未配置，默认值为4次尝试
   *  
   * @return 每个Map任务的最大尝试次数
   */
  public int getMaxMapAttempts();

  /** 
   * 获取配置的Reduce任务最大尝试次数，对应配置项<code>mapred.reduce.max.attempts</code>
   * 如果未配置，默认值为4次尝试
   * 
   * @return 每个Reduce任务的最大尝试次数
   */
  public int getMaxReduceAttempts();

}