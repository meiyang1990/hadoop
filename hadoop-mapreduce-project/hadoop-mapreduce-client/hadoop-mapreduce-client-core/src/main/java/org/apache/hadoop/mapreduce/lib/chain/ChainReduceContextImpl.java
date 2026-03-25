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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

/**
 * @file org/apache/hadoop/mapreduce/lib/chain/ChainReduceContextImpl.java
 * @brief 链式Reduce处理的ReduceContext包装实现，属于MapReduce链式计算模块
 * 
 * 该类为ChainReducer提供自定义上下文实现，将大部分方法委托给原始ReduceContext，
 * 仅覆盖配置获取和输出写入相关方法，支持链式处理中每个Reducer阶段使用独立配置
 * 和自定义输出接收器。
 */
/**
 * A simple wrapper class that delegates most of its functionality to the
 * underlying context, but overrides the methods to do with record writer and
 * configuration
 */
/**
 * 链式Reduce处理的ReduceContext包装类
 * 职责：将大部分方法委托给原始上下文，仅替换配置和输出写入逻辑，支持链式Reduce管道中多阶段处理
 * @param <KEYIN> 输入Key类型
 * @param <VALUEIN> 输入Value类型
 * @param <KEYOUT> 输出Key类型
 * @param <VALUEOUT> 输出Value类型
 */
class ChainReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
    ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  // 原始基础ReduceContext，大部分方法委托给它处理
  private final ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> base;
  // 当前阶段使用的自定义RecordWriter，用于输出当前Reduce阶段结果
  private final RecordWriter<KEYOUT, VALUEOUT> rw;
  // 当前阶段使用的自定义配置
  private final Configuration conf;

  /**
   * 构造ChainReduceContextImpl包装实例
   * @param base 原始基础ReduceContext
   * @param output 当前阶段使用的自定义RecordWriter
   * @param conf 当前阶段使用的自定义配置
   */
  public ChainReduceContextImpl(
      ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> base,
      RecordWriter<KEYOUT, VALUEOUT> output, Configuration conf) {
    this.base = base;
    this.rw = output;
    this.conf = conf;
  }

  @Override
  public Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
    return base.getValues();
  }

  @Override
  public boolean nextKey() throws IOException, InterruptedException {
    return base.nextKey();
  }

  @Override
  public Counter getCounter(Enum<?> counterName) {
    return base.getCounter(counterName);
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return base.getCounter(groupName, counterName);
  }

  @Override
  public KEYIN getCurrentKey() throws IOException, InterruptedException {
    return base.getCurrentKey();
  }

  @Override
  public VALUEIN getCurrentValue() throws IOException, InterruptedException {
    return base.getCurrentValue();
  }

  @Override
  public OutputCommitter getOutputCommitter() {
    return base.getOutputCommitter();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return base.nextKeyValue();
  }

  @Override
  /**
   * 使用当前阶段自定义RecordWriter写入输出键值对
   */
  public void write(KEYOUT key, VALUEOUT value) throws IOException,
      InterruptedException {
    rw.write(key, value);
  }

  @Override
  public String getStatus() {
    return base.getStatus();
  }

  @Override
  public TaskAttemptID getTaskAttemptID() {
    return base.getTaskAttemptID();
  }

  @Override
  public void setStatus(String msg) {
    base.setStatus(msg);
  }

  @Override
  public Path[] getArchiveClassPaths() {
    return base.getArchiveClassPaths();
  }

  @Override
  public String[] getArchiveTimestamps() {
    return base.getArchiveTimestamps();
  }

  @Override
  public URI[] getCacheArchives() throws IOException {
    return base.getCacheArchives();
  }

  @Override
  public URI[] getCacheFiles() throws IOException {
    return base.getCacheFiles();
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
      throws ClassNotFoundException {
    return base.getCombinerClass();
  }

  @Override
  /**
   * 返回当前Reduce阶段自定义配置
   */
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public Path[] getFileClassPaths() {
    return base.getFileClassPaths();
  }

  @Override
  public String[] getFileTimestamps() {
    return base.getFileTimestamps();
  }

  @Override
  public RawComparator<?> getCombinerKeyGroupingComparator() {
    return base.getCombinerKeyGroupingComparator();
  }

  @Override
  public RawComparator<?> getGroupingComparator() {
    return base.getGroupingComparator();
  }

  @Override
  public Class<? extends InputFormat<?, ?>> getInputFormatClass()
      throws ClassNotFoundException {
    return base.getInputFormatClass();
  }

  @Override
  public String getJar() {
    return base.getJar();
  }

  @Override
  public JobID getJobID() {
    return base.getJobID();
  }

  @Override
  public String getJobName() {
    return base.getJobName();
  }

  @Override
  public boolean getJobSetupCleanupNeeded() {
    return base.getJobSetupCleanupNeeded();
  }

  @Override
  public boolean getTaskCleanupNeeded() {
    return base.getTaskCleanupNeeded();
  }

  @Override
  public Path[] getLocalCacheArchives() throws IOException {
    return base.getLocalCacheArchives();
  }

  @Override
  public Path[] getLocalCacheFiles() throws IOException {
    return base.getLocalCacheFiles();
  }

  @Override
  public Class<?> getMapOutputKeyClass() {
    return base.getMapOutputKeyClass();
  }

  @Override
  public Class<?> getMapOutputValueClass() {
    return base.getMapOutputValueClass();
  }

  @Override
  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
      throws ClassNotFoundException {
    return base.getMapperClass();
  }

  @Override
  public int getMaxMapAttempts() {
    return base.getMaxMapAttempts();
  }

  @Override
  public int getMaxReduceAttempts() {
    return base.getMaxMapAttempts();
  }

  @Override
  public int getNumReduceTasks() {
    return base.getNumReduceTasks();
  }

  @Override
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
      throws ClassNotFoundException {
    return base.getOutputFormatClass();
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return base.getOutputKeyClass();
  }

  @Override
  public Class<?> getOutputValueClass() {
    return base.getOutputValueClass();
  }

  @Override
  public Class<? extends Partitioner<?, ?>> getPartitionerClass()
      throws ClassNotFoundException {
    return base.getPartitionerClass();
  }

  @Override
  public boolean getProfileEnabled() {
    return base.getProfileEnabled();
  }

  @Override
  public String getProfileParams() {
    return base.getProfileParams();
  }

  @Override
  public IntegerRanges getProfileTaskRange(boolean isMap) {
    return base.getProfileTaskRange(isMap);
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
      throws ClassNotFoundException {
    return base.getReducerClass();
  }

  @Override
  public RawComparator<?> getSortComparator() {
    return base.getSortComparator();
  }

  @Override
  public boolean getSymlink() {
    return base.getSymlink();
  }

  @Override
  public String getUser() {
    return base.getUser();
  }

  @Override
  public Path getWorkingDirectory() throws IOException {
    return base.getWorkingDirectory();
  }

  @Override
  public void progress() {
    base.progress();
  }

  @Override
  public Credentials getCredentials() {
    return base.getCredentials();
  }
  
  @Override
  public float getProgress() {
    return base.getProgress();
  }
}